package com.hypertino.transport.resolvers

import com.hypertino.hyperbus.transport.api.NoTransportRouteException
import com.hypertino.hyperbus.transport.resolvers.PlainEndpoint
import com.orbitz.consul.Consul
import monix.eval.Task
import monix.execution.Ack.Continue
import monix.execution.Scheduler
import monix.execution.cancelables.BooleanCancelable
import monix.reactive.subjects.ConcurrentSubject
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.duration._

class ConsulServiceResolverSpec extends FlatSpec with ScalaFutures with Matchers {
  val consul = Consul.builder().build()
  import monix.execution.Scheduler.Implicits.global

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(2, Seconds), interval = Span(100, Millis))

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

  "Service" should "update on change" in {
    val agentClient = consul.agentClient
    val serviceName = "test-service"
    val serviceId = "1"

    agentClient.register(15533, 3L, serviceName, serviceId)
    agentClient.pass(serviceId)
    Thread.sleep(200)

    val r = new ConsulServiceResolver(consul)

    Thread.sleep(200)

    val f = r.serviceObservable("test-service").take(2).toListL.runAsync

    Task.eval {
      //agentClient.fail(serviceId)
      agentClient.deregister("1")
    } delayExecution 150.milliseconds runAsync

    f.futureValue should equal(Seq(Seq(PlainEndpoint("127.0.0.1", Some(15533))), Seq.empty))
  }
}
