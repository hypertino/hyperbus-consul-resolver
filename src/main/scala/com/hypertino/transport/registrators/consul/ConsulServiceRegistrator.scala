/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.transport.registrators.consul

import com.hypertino.binders.config.ConfigBinders._
import com.hypertino.hyperbus.model.{HRL, HeaderHRL}
import com.hypertino.hyperbus.transport.api.ServiceRegistrator
import com.hypertino.hyperbus.transport.api.matchers.{RequestMatcher, Specific}
import com.hypertino.transport.util.consul.{ConsulConfigLoader, ConsulServiceMap}
import com.orbitz.consul.model.agent.{ImmutableRegistration, Registration}
import com.orbitz.consul.{Consul, NotRegisteredException}
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.atomic.AtomicInt
import monix.execution.{Cancelable, Scheduler}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.control.NonFatal

case class ConsulServiceRegistratorConfig(
                                        nodeId: String,
                                        address: Option[String] = None,
                                        port: Option[Int],
                                        serviceMap: ConsulServiceMap = ConsulServiceMap.empty,
                                        updateInterval: FiniteDuration = 3.seconds
                                      )

class ConsulServiceRegistratorException(message: String) extends RuntimeException(message)

class ConsulServiceRegistrator(consul: Consul, registratorConfig: ConsulServiceRegistratorConfig)
                              (implicit val scheduler: Scheduler) extends ServiceRegistrator with StrictLogging {

  private val serviceUpdaters = mutable.Map[(String, String), ServiceUpdater]()
  private val lock = new Object

  def this(config: Config)(implicit scheduler: Scheduler) = this(
    ConsulConfigLoader(config),
    config.read[ConsulServiceRegistratorConfig]("service-registrator")
  )

  override def registerService(requestMatcher: RequestMatcher): Task[Cancelable] = {
    requestMatcher.headers.get(HeaderHRL.FULL_HRL) match {
      case Some(Specific(url) :: tail) if tail.isEmpty ⇒
        val hrl = HRL.fromURL(url)
        Task.now {
          val serviceName = registratorConfig.serviceMap.mapService(hrl.service.get).getOrElse(hrl.service.get)
          val serviceId = serviceName + "-" + registratorConfig.nodeId
          val updater = lock.synchronized {
            val updater = serviceUpdaters.getOrElseUpdate((serviceName, serviceId), new ServiceUpdater(serviceName, serviceId))
            updater.addRef()
            updater
          }

          new Cancelable {
            override def cancel(): Unit = {
              lock.synchronized {
                if (updater.release()) {
                  updater.close()
                  serviceUpdaters.remove((serviceName, serviceId))
                }
              }
            }
          }
        }

      case None ⇒ Task.raiseError(new ConsulServiceRegistratorException(s"Can't register service without specific HRL: $requestMatcher"))
    }
  }

  class ServiceUpdater(serviceName: String, serviceId: String) extends AutoCloseable {
    private var isRegistered = false
    private val refCounter = AtomicInt(0)
    @volatile private var schedulerCancelable: Cancelable = Cancelable.empty

    def addRef(): Unit = {
      if (refCounter.getAndIncrement() == 0) {
        schedulerCancelable = scheduler.scheduleWithFixedDelay(0.seconds,registratorConfig.updateInterval/3)(this.update())
      }
    }

    def release(): Boolean ={
      refCounter.decrementAndGet() <= 0
    }

    private def update(): Unit = {
      try {
        val agentClient = consul.agentClient
        if (!isRegistered) {
          logger.info(s"Registering service in consul: $serviceName [$serviceId] with config $registratorConfig")
          val registrationBuilder = ImmutableRegistration
            .builder()
            .check(Registration.RegCheck.ttl(registratorConfig.updateInterval.toSeconds))
            .name(serviceName)
            .id(serviceId)

          registratorConfig.port.foreach(registrationBuilder.port)
          registratorConfig.address.foreach(registrationBuilder.address)

          agentClient.register(registrationBuilder.build())
          isRegistered = true
        }
        else {
          logger.debug(s"Updating service in consul: $serviceName [$serviceId]")
          agentClient.pass(serviceId)
        }
      }
      catch {
        case ne: NotRegisteredException ⇒
          logger.error(s"Service $serviceName [$serviceId] is not registered in consul", ne)
          isRegistered = false

        case NonFatal(e) ⇒
          logger.error(s"Can't update service $serviceName [$serviceId] in consul", e)
      }
    }

    override def close(): Unit = {
      try {
        logger.info(s"Deregistering service $serviceName [$serviceId]")
        schedulerCancelable.cancel()
        val agentClient = consul.agentClient
        agentClient.deregister(serviceId)
      }
      catch {
        case NonFatal(e) ⇒
          logger.error(s"Can't update service $serviceName in consul", e)
      }
    }
  }
}
