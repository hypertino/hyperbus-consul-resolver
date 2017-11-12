/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.transport.util.consul

import com.google.common.net.HostAndPort
import com.orbitz.consul.Consul
import com.typesafe.config.Config

private[consul] case class BasicAuth(username: String, password: String)

object ConsulConfigLoader {
  def apply(config: Config): Consul = {
    import com.hypertino.binders.config.ConfigBinders._
    val builder = Consul.builder()
    if (config.hasPath("address")) {
      builder.withHostAndPort(HostAndPort.fromString(config.getString("address")))
    }
    if (config.hasPath("basic-auth")) {
      val auth = config.read[BasicAuth]("basic-auth")
      builder.withBasicAuth(auth.username,auth.password)
    }
    builder.build()
  }
}
