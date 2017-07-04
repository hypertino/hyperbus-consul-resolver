package com.hypertino.transport.resolvers

import com.hypertino.hyperbus.model.{DynamicRequest, EmptyBody, HRL, Method}
import com.hypertino.hyperbus.transport.api.NoTransportRouteException
import com.hypertino.hyperbus.transport.resolvers.PlainEndpoint
import com.orbitz.consul.Consul
import monix.execution.Ack.Continue
import monix.execution.atomic.AtomicInt
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class ConsulServiceResolverSpec extends FlatSpec with ScalaFutures with Matchers with Eventually with BeforeAndAfterAll {
  import monix.execution.Scheduler.Implicits.global
  val consul = Consul.builder().build()

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(4, Seconds), interval = Span(300, Millis))

  "Service" should "resolve" in {
    val agentClient = consul.agentClient
    val serviceName = "test-service-1"
    val serviceId = "1"

    agentClient.register(15533, 3L, serviceName, serviceId)
    agentClient.pass(serviceId)

    val r = new ConsulServiceResolver(consul)
    r.lookupService(req(serviceName)).runAsync.futureValue should equal(PlainEndpoint("127.0.0.1", Some(15533)))

    agentClient.deregister(serviceId)
  }

  "Not existing service" should "fail fast" in {
    val r = new ConsulServiceResolver(consul)
    r.lookupService(req("test-service")).runAsync.failed.futureValue shouldBe a[NoTransportRouteException]
  }

  "Service" should "update on change" in {
    val agentClient = consul.agentClient
    val serviceName = "test-service-3"
    val serviceId = "3"
    agentClient.deregister(serviceId)

    agentClient.register(15533, 3L, serviceName, serviceId)
    agentClient.pass(serviceId)

    @volatile var becomeAlive = false
    @volatile var becomeDead = false
    val lock = new Object
    val r = new ConsulServiceResolver(consul)

    val c = r.serviceObservable(req(serviceName)).subscribe(seq ⇒ {
      lock.synchronized {
        if (seq.nonEmpty) {
          agentClient.fail(serviceId)
          seq should equal(Seq(PlainEndpoint("127.0.0.1", Some(15533))))
          becomeAlive = true
        }
        else if (becomeAlive) {
          seq shouldBe empty
          agentClient.deregister("3")
          becomeDead = true
        }
      }
      Continue
    })

    eventually {
      lock.synchronized {
        if (!becomeAlive) {
          agentClient.pass(serviceId)
        }
      }
      becomeAlive && becomeDead
    }
    c.cancel()
    agentClient.deregister(serviceId)
  }

  override def afterAll(): Unit = {
    consul.destroy()
  }

  def req(serviceName: String): DynamicRequest = {
    import com.hypertino.hyperbus.model.MessagingContext.Implicits.emptyContext
    DynamicRequest(HRL(s"hb://$serviceName"), Method.GET, EmptyBody)
  }
}
