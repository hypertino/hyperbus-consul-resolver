/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

import com.hypertino.hyperbus.transport.api.matchers.{RegexMatcher, Specific}
import com.hypertino.transport.util.consul.ConsulServiceMap
import com.typesafe.config.ConfigFactory
import org.scalatest.{FlatSpec, Matchers}

class ConsulServiceMapSpec extends FlatSpec with Matchers {
  "ConsulServiceMap" should "matchService" in {
    val m = ConsulServiceMap(Seq(
      Specific("a") → "b",
      RegexMatcher("abc(.*)") → "x$1"
    ))

    m.mapService("a") shouldBe Some("b")
    m.mapService("ab") shouldBe None
    m.mapService("abcde") shouldBe Some("xde")
    m.mapService("qbcde") shouldBe None
  }

  it should "load from config" in {
    val config = ConfigFactory.parseString(
      """
        service-map: {
          "a": "b"
          "~abc(.*)": "x$1"
        }
        """)

    val m = ConsulServiceMap(config, "service-map")
    m shouldBe ConsulServiceMap(Seq(
      Specific("a") → "b",
      RegexMatcher("abc(.*)") → "x$1"
    ))
  }
}
