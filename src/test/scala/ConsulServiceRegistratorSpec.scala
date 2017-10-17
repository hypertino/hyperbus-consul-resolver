import com.hypertino.hyperbus.model.Method
import com.hypertino.hyperbus.transport.api.NoTransportRouteException
import com.hypertino.hyperbus.transport.api.matchers.RequestMatcher
import com.hypertino.hyperbus.transport.resolvers.PlainEndpoint
import com.hypertino.transport.registrators.consul.{ConsulServiceRegistrator, ConsulServiceResolverConfig}
import com.hypertino.transport.resolvers.consul.ConsulServiceResolver
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class ConsulServiceRegistratorSpec extends FlatSpec with ScalaFutures with Matchers with Eventually with BeforeAndAfterAll with TestHelper {

  import monix.execution.Scheduler.Implicits.global

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(4, Seconds), interval = Span(300, Millis))

  "Service" should "register" in {
    val requestMatcher = RequestMatcher("hb://user", Method.GET)
    val serviceRegistrator = new ConsulServiceRegistrator(consul, ConsulServiceResolverConfig(123,"node-1"))
    val cancelable = serviceRegistrator.registerService(requestMatcher).runAsync.futureValue
    try {

      val r = new ConsulServiceResolver(consul)
      eventually {
        r.lookupService(req("user")).runAsync.futureValue should equal(PlainEndpoint("127.0.0.1", Some(123)))
      }
    }
    finally {
      cancelable.cancel()
    }
  }

  it should "deregister" in {
    val requestMatcher = RequestMatcher("hb://user", Method.GET)
    val serviceRegistrator = new ConsulServiceRegistrator(consul, ConsulServiceResolverConfig(123,"node-1"))
    val cancelable = serviceRegistrator.registerService(requestMatcher).runAsync.futureValue
    val r = new ConsulServiceResolver(consul)
    try {
      eventually {
        r.lookupService(req("user")).runAsync.futureValue should equal(PlainEndpoint("127.0.0.1", Some(123)))
      }
    }
    finally {
      cancelable.cancel()
    }

    eventually {
      r.lookupService(req("user")).runAsync.failed.futureValue shouldBe a[NoTransportRouteException]
    }
  }
}
