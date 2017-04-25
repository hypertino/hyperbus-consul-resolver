package com.hypertino.transport.resolvers

import java.util

import com.hypertino.hyperbus.transport.api.{ServiceEndpoint, ServiceResolver}
import com.hypertino.hyperbus.transport.resolvers.PlainEndpoint
import com.orbitz.consul.Consul
import com.orbitz.consul.cache.{ConsulCache, ServiceHealthCache, ServiceHealthKey}
import com.orbitz.consul.model.health.ServiceHealth
import monix.execution.Scheduler
import monix.reactive.Observable
import monix.reactive.subjects.ConcurrentSubject

class ConsulServiceResolver(consul: Consul)(implicit val scheduler: Scheduler) extends ServiceResolver {

  def this()(implicit scheduler: Scheduler) = this(Consul.builder().build())

  override def serviceObservable(serviceName: String): Observable[Seq[ServiceEndpoint]] = {
    val subject = ConcurrentSubject.publishToOne[Seq[ServiceEndpoint]]
    val healthClient = consul.healthClient

    val svHealth = ServiceHealthCache.newCache(healthClient, serviceName)
    val listener = new ConsulCache.Listener[ServiceHealthKey, ServiceHealth] {
      override def notify(newValues: util.Map[ServiceHealthKey, ServiceHealth]): Unit = {
        println(newValues)
        //val seq = Seq.canBuildFrom[ServiceEndpoint]
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

        subject.onNext(seq)
      }
    }
    svHealth.addListener(listener)
    svHealth.start()
    subject
  }

  private def isPassing(s: String) = s != null && (s.equalsIgnoreCase("passing") || s.equalsIgnoreCase("warning"))
}
