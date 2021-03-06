/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.transport.resolvers.consul

import java.util
import java.util.concurrent.{Callable, TimeUnit}

import com.google.common.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}
import com.hypertino.hyperbus.model.RequestBase
import com.hypertino.hyperbus.transport.api.{NoTransportRouteException, ServiceEndpoint, ServiceResolver}
import com.hypertino.hyperbus.transport.resolvers.PlainEndpoint
import com.hypertino.transport.util.consul.{ConsulConfiguration, ConsulServiceMap}
import com.orbitz.consul.Consul
import com.orbitz.consul.cache.{ConsulCache, ServiceHealthCache, ServiceHealthKey}
import com.orbitz.consul.model.health.ServiceHealth
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.execution.{Cancelable, Scheduler}
import monix.execution.atomic.AtomicAny
import monix.reactive.Observable
import monix.reactive.subjects.ConcurrentSubject
import com.hypertino.binders.config.ConfigBinders._
import com.orbitz.consul.option.{ConsistencyMode, ImmutableQueryOptions}
import monix.eval.Task

import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.{Failure, Success}

private [consul] case class LC(subscription: Cancelable,
                               healthCache: ServiceHealthCache,
                               latestTask: AtomicAny[Task[Seq[ServiceEndpoint]]]
                              )

case class ConsulServiceResolverConfig(
                                        consul: ConsulConfiguration,
                                        serviceMap: ConsulServiceMap = ConsulServiceMap.empty,
                                        cachePeriod: FiniteDuration = 60.seconds,
                                        consistencyMode: String = ConsistencyMode.CONSISTENT.toString,
                                        watchTime: FiniteDuration = 10.seconds
                                      )

class ConsulServiceResolver(val consul: Consul, resolverConfig: ConsulServiceResolverConfig)
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
    .expireAfterAccess(resolverConfig.cachePeriod.toMillis, TimeUnit.MILLISECONDS)
    .removalListener(new RemovalListener[String, LC] {
      override def onRemoval(notification: RemovalNotification[String, LC]) = {
        val lc = notification.getValue
        if (lc != null) {
          lc.subscription.cancel()
        }
      }
    })
    .build[String,LC]()

  def this(resolverConfig: ConsulServiceResolverConfig)(implicit scheduler: Scheduler) = this(
    resolverConfig.consul.buildClient(),
    resolverConfig
  )

  def this(config: Config)(implicit scheduler: Scheduler) = this(
    config.root.read[ConsulServiceResolverConfig]
  )

  override def lookupServiceEndpoints(message: RequestBase): Task[Seq[ServiceEndpoint]] = {
    message.headers.hrl.service.map { serviceName ⇒
      val c = lookupCache.get(serviceName, new Callable[LC] {
        override def call() = {
          val promise = Promise[Seq[ServiceEndpoint]]
          val cancelable = serviceObservable(message)
            .doOnStart(passing ⇒ promise.complete(Success(passing)))
            .subscribe()
          val svHealth = healthCaches.getIfPresent(serviceName)
          val task = Task.fromFuture(promise.future).memoize
          if (svHealth != null) {
            LC(cancelable, svHealth, AtomicAny(task))
          }
          else {
            cancelable.cancel()
            throw new IllegalStateException("svHealth should not be null")
          }
        }
      })

      c.latestTask.get
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
          val queryOptions = ImmutableQueryOptions.builder()
            .consistencyMode(ConsistencyMode.valueOf(resolverConfig.consistencyMode))
            .build()

          val v = ServiceHealthCache.newCache(
            healthClient,
            consulServiceName,
            true,
            resolverConfig.watchTime.toSeconds.toInt,
            queryOptions
          )
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
            lc.latestTask.set(Task.now(passing).memoize)
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