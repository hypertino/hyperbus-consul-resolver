package com.hypertino.transport.resolvers.consul

import java.util
import java.util.concurrent.{Callable, TimeUnit}

import com.google.common.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}
import com.hypertino.hyperbus.model.RequestBase
import com.hypertino.hyperbus.transport.api.{NoTransportRouteException, ServiceEndpoint, ServiceResolver}
import com.hypertino.hyperbus.transport.resolvers.PlainEndpoint
import com.hypertino.transport.util.consul.{ConsulConfigLoader, ConsulServiceMap}
import com.orbitz.consul.Consul
import com.orbitz.consul.cache.{ConsulCache, ServiceHealthCache, ServiceHealthKey}
import com.orbitz.consul.model.health.ServiceHealth
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.execution.{Cancelable, Scheduler}
import monix.execution.atomic.{AtomicAny, AtomicInt}
import monix.reactive.Observable
import monix.reactive.subjects.ConcurrentSubject
import com.hypertino.binders.config.ConfigBinders._
import monix.eval.Task

import scala.util.{Failure, Success}

private [consul] case class LC(subscription: Cancelable, healthCache: ServiceHealthCache, latest: AtomicAny[Option[Seq[ServiceEndpoint]]])

case class ConsulServiceResolverConfig(
                                        serviceMap: ConsulServiceMap = ConsulServiceMap.empty,
                                        cachePeriodInSeconds: Int = 60
                                      )

class ConsulServiceResolver(consul: Consul, resolverConfig: ConsulServiceResolverConfig)
                           (implicit val scheduler: Scheduler) extends ServiceResolver with StrictLogging {

  private val healthCaches: Cache[String, ServiceHealthCache] = CacheBuilder.newBuilder()
    .weakValues()
    .removalListener(new RemovalListener[String, ServiceHealthCache] {
      override def onRemoval(notification: RemovalNotification[String, ServiceHealthCache]) = {
        val svHealth = notification.getValue
        if (svHealth != null) {
          svHealth.stop()
        }
      }
    })
    .build[String,ServiceHealthCache]()

  private val lookupCache: Cache[String, LC] = CacheBuilder.newBuilder()
    .expireAfterAccess(resolverConfig.cachePeriodInSeconds, TimeUnit.SECONDS)
    .removalListener(new RemovalListener[String, LC] {
      override def onRemoval(notification: RemovalNotification[String, LC]) = {
        val lc = notification.getValue
        if (lc != null) {
          lc.subscription.cancel()
        }
      }
    })
    .build[String,LC]()

  def this(config: Config)(implicit scheduler: Scheduler) = this(
    ConsulConfigLoader(config),
    config.read[ConsulServiceResolverConfig]("service-resolver")
  )

  override def lookupServiceEndpoints(message: RequestBase): Task[Seq[ServiceEndpoint]] = {
    message.headers.hrl.service.map { serviceName ⇒
      logger.trace(s"lookupServiceEndpoints for $serviceName")
      val c = lookupCache.get(serviceName, new Callable[LC] {
        override def call() = {
          val cancelable = serviceObservable(message).subscribe()
          val svHealth = healthCaches.getIfPresent(serviceName)
          if (svHealth != null) {
            LC(cancelable, svHealth, AtomicAny(None))
          }
          else {
            cancelable.cancel()
            throw new IllegalStateException("svHealth should not be null")
          }
        }
      })

      c.latest.get.map { passing ⇒
        Task.now(passing)
      } getOrElse {
        Task.create[Seq[ServiceEndpoint]] { (_, callback) ⇒
          val cancelableListener = new ConsulCache.Listener[ServiceHealthKey, ServiceHealth] with Cancelable {
            override def notify(newValues: util.Map[ServiceHealthKey, ServiceHealth]): Unit = {
              val passing = ConsulServiceResolverUtil.passing(newValues)
              logger.trace(s"Consul services notification while resolving for $serviceName: $passing")
              c.healthCache.removeListener(this)
              callback(Success(passing))
            }

            override def cancel(): Unit = {
              c.healthCache.removeListener(this)
            }
          }
          c.healthCache.addListener(cancelableListener)
          cancelableListener
        }
      }
    } getOrElse {
      Task.raiseError(
        new NoTransportRouteException(s"Request doesn't specify service name: ${message.headers.hrl}")
      )
    }
  }

  override def serviceObservable(message: RequestBase): Observable[Seq[ServiceEndpoint]] = {
    message.headers.hrl.service.map { serviceName ⇒
      logger.trace(s"serviceObservable for $serviceName")
      val consulServiceName = resolverConfig.serviceMap.mapService(serviceName).getOrElse(serviceName)
      val subject = ConcurrentSubject.publishToOne[Seq[ServiceEndpoint]]
      val healthClient = consul.healthClient

      val svHealth = healthCaches.get(serviceName, new Callable[ServiceHealthCache] {
        override def call() = {
          val v = ServiceHealthCache.newCache(healthClient, consulServiceName)
          v.start()
          v
        }
      })

      val listener = new ConsulCache.Listener[ServiceHealthKey, ServiceHealth] {
        override def notify(newValues: util.Map[ServiceHealthKey, ServiceHealth]): Unit = {
          val passing = ConsulServiceResolverUtil.passing(newValues)
          logger.trace(s"Consul services notification for $serviceName/$consulServiceName: $passing")
          val lc = lookupCache.getIfPresent(serviceName)
          if (lc != null) {
            lc.latest.set(Some(passing))
          }
          subject.onNext(passing)
        }
      }
      subject
        .doAfterSubscribe(() ⇒ svHealth.addListener(listener))
        .doOnTerminate(_ ⇒ svHealth.removeListener(listener))
    } getOrElse {
      Observable.raiseError(
        new NoTransportRouteException(s"Request doesn't specify service name: ${message.headers.hrl}")
      )
    }
  }
}

private [consul] object ConsulServiceResolverUtil {
  def passing(newValues: util.Map[ServiceHealthKey, ServiceHealth]): Seq[PlainEndpoint] = {
    import scala.collection.JavaConverters._
    newValues
      .asScala
      .filter {
        _._2
          .getChecks
          .asScala
          .forall(check ⇒ isPassing(check.getStatus))
      }
      .map { i ⇒
        PlainEndpoint(i._1.getHost, Some(i._1.getPort))
      }
      .toSeq
  }

  private def isPassing(s: String) = s != null && (s.equalsIgnoreCase("passing") || s.equalsIgnoreCase("warning"))
}