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
import scala.concurrent.duration._

case class BasicAuth(username: String, password: String)

case class ConsulConfiguration(
                              address: Option[String],
                              basicAuth: Option[BasicAuth],
                              connectTimeout: Option[FiniteDuration],
                              readTimeout: Option[FiniteDuration],
                              writeTimeout: Option[FiniteDuration]
                              )
{
  def buildClient(): Consul = {
    val builder = Consul.builder()
    address.foreach { address ⇒
      builder.withHostAndPort(HostAndPort.fromString(address))
    }
    basicAuth.foreach { basicAuth ⇒
      builder.withBasicAuth(basicAuth.username,basicAuth.password)
    }
    connectTimeout.foreach { connectTimeout ⇒
      builder.withConnectTimeoutMillis(connectTimeout.toMillis)
    }
    readTimeout.foreach { readTimeout ⇒
      builder.withReadTimeoutMillis(readTimeout.toMillis)
    }
    writeTimeout.foreach { writeTimeout ⇒
      builder.withWriteTimeoutMillis(writeTimeout.toMillis)
    }
    builder.build()
  }
}
