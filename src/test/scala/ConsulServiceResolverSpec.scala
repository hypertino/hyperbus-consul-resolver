/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

import com.hypertino.hyperbus.transport.api.NoTransportRouteException
import com.hypertino.hyperbus.transport.api.matchers.RegexMatcher
import com.hypertino.hyperbus.transport.resolvers.PlainEndpoint
import com.hypertino.transport.resolvers.consul.{ConsulServiceResolver, ConsulServiceResolverConfig}
import com.hypertino.transport.util.consul.ConsulServiceMap
import monix.execution.Ack.Continue
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import scala.concurrent.duration._

class ConsulServiceResolverSpec extends FlatSpec with ScalaFutures with Matchers with Eventually with BeforeAndAfterAll with TestHelper {
  import monix.execution.Scheduler.Implicits.global

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(4, Seconds), interval = Span(300, Millis))

  "Service" should "resolve" in {
    val agentClient = consul.agentClient
    val serviceName = "test-service-1"
    val serviceId = "1"

    agentClient.register(15533, 10L, "hb-" + serviceName, serviceId)
    agentClient.pass(serviceId)

    val r = new ConsulServiceResolver(consul, csrConfig)
    eventually {
      agentClient.pass(serviceId)
      r.lookupService(req(serviceName)).runAsync.futureValue should equal(PlainEndpoint("127.0.0.1", Some(15533)))
    }
    agentClient.deregister(serviceId)
  }

  it should "resolve when cache expires" in {
    val agentClient = consul.agentClient
    val serviceName = "test-service-2"
    val serviceId = "2"

    agentClient.register(15533, 20L, "hb-" + serviceName, serviceId)
    agentClient.pass(serviceId)

    val r = new ConsulServiceResolver(consul, csrConfig.copy(cachePeriod=5.seconds))
    eventually {
      r.lookupService(req(serviceName)).runAsync.futureValue should equal(PlainEndpoint("127.0.0.1", Some(15533)))
    }
    Thread.sleep(1500)
    val count = 10000
    println(s"Making $count lookups (should be cached) at ${new java.util.Date()}")
    0 to 10000 map { _ ⇒
      r.lookupService(req(serviceName)).runAsync.futureValue should equal(PlainEndpoint("127.0.0.1", Some(15533)))
    }
    println(s"Completed $count lookups at ${new java.util.Date()}")
    Thread.sleep(1500)
    r.lookupService(req(serviceName)).runAsync.futureValue should equal(PlainEndpoint("127.0.0.1", Some(15533)))
    Thread.sleep(5000)
    r.lookupService(req(serviceName)).runAsync.futureValue should equal(PlainEndpoint("127.0.0.1", Some(15533)))
    agentClient.deregister(serviceId)
  }

  "Not existing service" should "fail fast" in {
    val r = new ConsulServiceResolver(consul, ConsulServiceResolverConfig(ConsulServiceMap.empty))
    r.lookupService(req("test-service")).runAsync.failed.futureValue shouldBe a[NoTransportRouteException]
  }

  "Service" should "update on change" in {
    val agentClient = consul.agentClient
    val serviceName = "test-service-3"
    val serviceId = "3"
    agentClient.deregister(serviceId)

    agentClient.register(15533, 3L, "hb-" + serviceName, serviceId)
    agentClient.pass(serviceId)

    @volatile var becomeAlive = false
    @volatile var becomeDead = false
    val lock = new Object
    val r = new ConsulServiceResolver(consul, csrConfig)

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

  def csrConfig = ConsulServiceResolverConfig(ConsulServiceMap(Seq(RegexMatcher("^(.+)$") → "hb-$1")))
}
