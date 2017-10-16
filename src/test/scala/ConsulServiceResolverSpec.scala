package abc

import com.hypertino.hyperbus.transport.api.NoTransportRouteException
import com.hypertino.hyperbus.transport.resolvers.PlainEndpoint
import com.hypertino.transport.resolvers.consul.ConsulServiceResolver
import monix.execution.Ack.Continue
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class ConsulServiceResolverSpec extends FlatSpec with ScalaFutures with Matchers with Eventually with BeforeAndAfterAll with TestHelper {
  import monix.execution.Scheduler.Implicits.global

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
}