package eu.inn.revault.modules

import com.datastax.driver.core.Session
import com.typesafe.config.Config
import eu.inn.revault.{CassandraConnector, RevaultService}
import eu.inn.servicecontrol.ConsoleModule
import eu.inn.servicecontrol.api.Service

import scala.concurrent.ExecutionContext

class ServiceModule(config: Config) extends ConsoleModule {
  bind [Config] to config
  bind [ExecutionContext] to scala.concurrent.ExecutionContext.Implicits.global
  bind [CassandraConnector] to new CassandraConnector {
    override def connect(): Session = {
      CassandraConnector.createCassandraSession(config.getConfig("cassandra"))
    }
  }
  bind [Service] to injected [RevaultService]
}
