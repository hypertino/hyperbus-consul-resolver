import com.hypertino.hyperbus.model.{DynamicRequest, EmptyBody, HRL, Method}
import com.orbitz.consul.Consul
import org.scalatest.{BeforeAndAfterAll, Suite}

trait TestHelper extends BeforeAndAfterAll {
  this: Suite ⇒
  val consul = Consul.builder().build()

  override def afterAll(): Unit = {
    consul.destroy()
  }

  def req(serviceName: String): DynamicRequest = {
    import com.hypertino.hyperbus.model.MessagingContext.Implicits.emptyContext
    DynamicRequest(HRL(s"hb://$serviceName"), Method.GET, EmptyBody)
  }
}
