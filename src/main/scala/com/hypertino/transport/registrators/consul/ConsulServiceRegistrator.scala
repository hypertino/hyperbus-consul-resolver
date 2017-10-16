package com.hypertino.transport.registrators.consul

import com.hypertino.binders.config.ConfigBinders._
import com.hypertino.hyperbus.model.{HRL, HeaderHRL}
import com.hypertino.hyperbus.transport.api.ServiceRegistrator
import com.hypertino.hyperbus.transport.api.matchers.{RequestMatcher, Specific}
import com.hypertino.transport.util.consul.ConsulConfigLoader
import com.orbitz.consul.model.agent.{ImmutableRegistration, Registration}
import com.orbitz.consul.{Consul, NotRegisteredException}
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.{Cancelable, Scheduler}

import scala.concurrent.duration._
import scala.util.control.NonFatal

case class ConsulServiceResolverConfig(
                                        port: Int,
                                        nodeId: String,
                                        updateInterval: FiniteDuration = 3.seconds,
                                      )

class ConsulServiceRegistratorException(message: String) extends RuntimeException(message)

class ConsulServiceRegistrator(consul: Consul, resolverConfig: ConsulServiceResolverConfig)
                              (implicit val scheduler: Scheduler) extends ServiceRegistrator with StrictLogging {

  def this(config: Config)(implicit scheduler: Scheduler) = this(
    ConsulConfigLoader(config),
    config.read[ConsulServiceResolverConfig]("service-resolver")
  )

  override def registerService(requestMatcher: RequestMatcher): Task[Cancelable] = {
    requestMatcher.headers.get(HeaderHRL.FULL_HRL) match {
      case Some(Specific(url) :: tail) if tail.isEmpty ⇒
        val hrl = HRL.fromURL(url)
        Task.now {
          val updater = new ServiceUpdater(hrl)
          val cancelable = scheduler.scheduleWithFixedDelay(0.seconds,resolverConfig.updateInterval/3) {
            updater.update()
          }

          new Cancelable {
            override def cancel(): Unit = {
              cancelable.cancel()
              updater.close()
            }
          }
        }

      case None ⇒ Task.raiseError(new ConsulServiceRegistratorException(s"Can't register service without specific HRL: $requestMatcher"))
    }
  }

  class ServiceUpdater(hrl: HRL) extends AutoCloseable {
    private val serviceName = "hb-" + hrl.service.get
    private val serviceId = serviceName + "-" + resolverConfig.nodeId
    private var isRegistered = false

    def update(): Unit = {
      try {
        val agentClient = consul.agentClient
        if (!isRegistered) {
          logger.info(s"Registering service in consul: $serviceName [$serviceId] with config $resolverConfig")
          val registration = ImmutableRegistration
            .builder()
            .port(resolverConfig.port)
            .check(Registration.RegCheck.ttl(resolverConfig.updateInterval.toSeconds))
            .name(serviceName)
            .id(serviceId)
            .build()

          agentClient.register(registration)
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