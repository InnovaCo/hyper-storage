package eu.inn.revault

import com.datastax.driver.core.Session
import com.typesafe.config.Config
import eu.inn.config.ConfigLoader
import eu.inn.servicecontrol.ConsoleModule
import eu.inn.servicecontrol.api.{Service, ServiceController}

import scala.concurrent.ExecutionContext

object MainApp extends ConsoleModule {
  bind [Config] to ConfigLoader()
  bind [ExecutionContext] to scala.concurrent.ExecutionContext.Implicits.global
  bind [CassandraConnector] to new CassandraConnector {
    override def connect(): Session = {
      val config = inject[Config]
      CassandraConnector.createCassandraSession(config.getConfig("cassandra"), "revault")
    }
  }
  bind [Service] to injected [RevaultService]

  def main(args: Array[String]): Unit = {
    inject[ServiceController].run()
  }
}
