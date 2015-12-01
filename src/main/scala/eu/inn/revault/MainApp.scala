package eu.inn.revault

import com.typesafe.config.Config
import eu.inn.config.ConfigLoader
import eu.inn.servicecontrol.ConsoleModule
import eu.inn.servicecontrol.api.{Service, ServiceController}

object MainApp extends ConsoleModule {
  bind [Config] to ConfigLoader()
  bind [Service] to injected [RevaultService]

  def main(args: Array[String]): Unit = {
    inject[ServiceController].run()
  }
}
