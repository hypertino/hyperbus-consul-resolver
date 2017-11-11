package com.hypertino.transport.util.consul

import com.hypertino.binders.config.ConfigDeserializer
import com.hypertino.binders.core.ImplicitDeserializer
import com.hypertino.hyperbus.transport.api.matchers.{RegexMatcher, TextMatcher}
import com.typesafe.config.{Config, ConfigObject}

case class ConsulServiceMap(matches: Seq[(TextMatcher, String)]) {
  def mapService(original: String): Option[String] = {
    var res: Option[String] = None
    var complete = false
    val it = matches.iterator
    while (it.hasNext && !complete) {
      val (m,replacement) = it.next()
      m match {
        case r: RegexMatcher ⇒
          r.replaceIfMatch(original, replacement).foreach { s ⇒
            res = Some(s)
            complete = true
          }

        case _ ⇒
          if (m.matchText(original)) {
            res = Some(replacement)
            complete = true
          }
      }
    }
    res
  }
}

object ConsulServiceMap {
  val empty: ConsulServiceMap = ConsulServiceMap(Seq.empty)
  implicit val consulServiceMapReader: ImplicitDeserializer[ConsulServiceMap, ConfigDeserializer[_]] = new ImplicitDeserializer[ConsulServiceMap, ConfigDeserializer[_]] {
    override def read(deserializer: ConfigDeserializer[_]): ConsulServiceMap = {
      deserializer.configValue.map { cv ⇒
        import com.hypertino.binders.config.ConfigBinders._
        ConsulServiceMap(cv.read[Map[String, String]].map { case (k: String, v: String) ⇒
          TextMatcher.fromCompactString(k) -> v
        }.toSeq)
      } getOrElse {
        ConsulServiceMap.empty
      }
    }
  }

  def apply(config: Config, path: String): ConsulServiceMap = {
    import com.hypertino.binders.config.ConfigBinders._
    val m = config.read[Map[String, String]](path)
    ConsulServiceMap(m.toSeq.map { case (k: String, v: String) ⇒
      TextMatcher.fromCompactString(k) -> v
    })
  }
}