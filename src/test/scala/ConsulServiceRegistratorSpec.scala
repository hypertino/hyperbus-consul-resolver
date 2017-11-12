/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

import com.hypertino.hyperbus.model.Method
import com.hypertino.hyperbus.transport.api.NoTransportRouteException
import com.hypertino.hyperbus.transport.api.matchers.{RegexMatcher, RequestMatcher, Specific}
import com.hypertino.hyperbus.transport.resolvers.PlainEndpoint
import com.hypertino.transport.registrators.consul.{ConsulServiceRegistrator, ConsulServiceRegistratorConfig}
import com.hypertino.transport.resolvers.consul.{ConsulServiceResolver, ConsulServiceResolverConfig}
import com.hypertino.transport.util.consul.ConsulServiceMap
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class ConsulServiceRegistratorSpec extends FlatSpec with ScalaFutures with Matchers with Eventually with BeforeAndAfterAll with TestHelper {

  import monix.execution.Scheduler.Implicits.global

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(300, Millis))

  "Service" should "register" in {
    val requestMatcher = RequestMatcher("hb://user", Method.GET)
    val serviceRegistrator = new ConsulServiceRegistrator(consul, ConsulServiceRegistratorConfig("node-1",None,Some(123),serviceMap))
    val cancelable = serviceRegistrator.registerService(requestMatcher).runAsync.futureValue
    try {

      val r = new ConsulServiceResolver(consul, csrConfig)
      eventually {
        r.lookupService(req("user")).runAsync.futureValue should equal(PlainEndpoint("127.0.0.1", Some(123)))
      }
    }
    finally {
      cancelable.cancel()
    }
  }

  it should "deregister" in {
    val requestMatcher = RequestMatcher("hb://auth", Method.GET)
    val serviceRegistrator = new ConsulServiceRegistrator(consul, ConsulServiceRegistratorConfig("node-1", None, Some(123),serviceMap))
    val cancelable = serviceRegistrator.registerService(requestMatcher).runAsync.futureValue
    val r = new ConsulServiceResolver(consul, csrConfig)
    try {
      eventually {
        r.lookupService(req("auth")).runAsync.futureValue should equal(PlainEndpoint("127.0.0.1", Some(123)))
      }
    }
    finally {
      cancelable.cancel()
    }

    eventually {
      r.lookupService(req("auth")).runAsync.failed.futureValue shouldBe a[NoTransportRouteException]
    }
  }

  it should "load config" in {
    val config = ConfigFactory.parseString(
      """
        service-resolver: {
          node-id: 1
          port: 12345
          service-map: {
            "a": "b"
            "~abc(.*)": "x$1"
          }
        }
      """)

    import com.hypertino.binders.config.ConfigBinders._
    val r = config.read[ConsulServiceRegistratorConfig]("service-resolver")
    r.nodeId shouldBe "1"
    r.port shouldBe Some(12345)
    r.serviceMap shouldBe ConsulServiceMap(Seq(
      Specific("a") → "b",
      RegexMatcher("abc(.*)") → "x$1"
    ))
  }

  def serviceMap = ConsulServiceMap(Seq(RegexMatcher("^(.*)$") → "hb-$1"))
  def csrConfig = ConsulServiceResolverConfig(serviceMap)
}
