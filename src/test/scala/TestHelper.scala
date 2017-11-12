/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

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
