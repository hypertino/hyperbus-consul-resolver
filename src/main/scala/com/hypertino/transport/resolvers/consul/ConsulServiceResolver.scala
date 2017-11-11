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

private [consul] case class LC(subscription: Cancelable, healthCache: ServiceHealthCache, latest: AtomicAny[Seq[ServiceEndpoint]])

case class ConsulServiceResolverConfig(
                                        serviceMap: ConsulServiceMap = ConsulServiceMap.empty
                                      )

class ConsulServiceResolver(consul: Consul, resolverConfig: ConsulServiceResolverConfig)
                           (implicit val scheduler: Scheduler) extends ServiceResolver with StrictLogging {

  private val healthCaches: Cache[String, ServiceHealthCache] = CacheBuilder.newBuilder()
    .expireAfterAccess(60, TimeUnit.SECONDS)
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
    .expireAfterAccess(60, TimeUnit.SECONDS)
    .removalListener(new RemovalListener[String, LC] {
      override def onRemoval(notification: RemovalNotification[String, LC]) = {
        val lc = notification.getValue
        if (lc != null) {
          lc.subscription.cancel()
          lc.healthCache.stop()
        }
      }
    })
    .build[String,LC]()

  def this(config: Config)(implicit scheduler: Scheduler) = this(
    ConsulConfigLoader(config),
    config.read[ConsulServiceResolverConfig]("service-resolver")
  )

  override def serviceObservable(message: RequestBase): Observable[Seq[ServiceEndpoint]] = {
    message.headers.hrl.service.map { serviceName ⇒
      val consulServiceName = resolverConfig.serviceMap.mapService(serviceName).getOrElse(serviceName)
      val subject = ConcurrentSubject.publishToOne[Seq[ServiceEndpoint]]
      val healthClient = consul.healthClient

      val svHealth = healthCaches.get(consulServiceName, new Callable[ServiceHealthCache] {
        override def call() = {
          val v = ServiceHealthCache.newCache(healthClient, consulServiceName)
          v.start()
          v
        }
      })

      val listener = new ConsulCache.Listener[ServiceHealthKey, ServiceHealth] {
        override def notify(newValues: util.Map[ServiceHealthKey, ServiceHealth]): Unit = {
          import scala.collection.JavaConverters._

          val seq = newValues
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
          logger.trace(s"Consul services notification for $serviceName: $seq")
          subject.onNext(seq)
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

  private def isPassing(s: String) = s != null && (s.equalsIgnoreCase("passing") || s.equalsIgnoreCase("warning"))
}
