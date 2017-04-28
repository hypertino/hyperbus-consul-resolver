package com.hypertino.transport.resolvers

import com.hypertino.hyperbus.transport.api.NoTransportRouteException
import com.hypertino.hyperbus.transport.resolvers.PlainEndpoint
import com.orbitz.consul.Consul
import monix.eval.Task
import monix.execution.Ack.Continue
import monix.execution.atomic.AtomicInt
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._

class ConsulServiceResolverSpec extends FlatSpec with ScalaFutures with Matchers with Eventually {
  val consul = Consul.builder().build()
  import monix.execution.Scheduler.Implicits.global

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(4, Seconds), interval = Span(300, Millis))

  "Service" should "resolve" in {
    val agentClient = consul.agentClient
    val serviceName = "test-service"
    val serviceId = "1"

    agentClient.register(15533, 3L, serviceName, serviceId)
    agentClient.pass(serviceId)

    val r = new ConsulServiceResolver(consul)
    r.lookupService("test-service").runAsync.futureValue should equal(PlainEndpoint("127.0.0.1", Some(15533)))

    agentClient.deregister("1")
  }

  "Not existing service" should "fail fast" ignore {
    val agentClient = consul.agentClient
    val serviceName = "unknown-service"
    val serviceId = "2"

    val r = new ConsulServiceResolver(consul)
    r.lookupService("test-service").runAsync.failed.futureValue shouldBe a[NoTransportRouteException]
  }

  "Service" should "update on change" in {
    val agentClient = consul.agentClient
    val serviceName = "test-service"
    val serviceId = "1"

    agentClient.register(15533, 3L, serviceName, serviceId)
    agentClient.pass(serviceId)

    val counter = AtomicInt(0)
    val r = new ConsulServiceResolver(consul)

    r.serviceObservable("test-service").subscribe(seq ⇒ {
      val v = counter.incrementAndGet()
      if (v == 1) {
        agentClient.fail(serviceId)
        seq should equal(Seq(PlainEndpoint("127.0.0.1", Some(15533))))
      }
      else {
        seq shouldBe empty
        agentClient.deregister("1")
      }
      Continue
    })

    eventually {
      counter.get should equal(2)
    }
    agentClient.deregister("1")
  }
}
