package eu.inn.hyperstorage

import eu.inn.hyperstorage.modules.ModuleAggregator
import eu.inn.servicecontrol.api.ServiceController
import scaldi.Injectable

object MainApp extends Injectable {
  def main(args: Array[String]): Unit = {
    implicit val injector = ModuleAggregator.injector
    inject[ServiceController].run()
  }
}
