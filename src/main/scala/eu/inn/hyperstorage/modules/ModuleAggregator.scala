package eu.inn.hyperstorage.modules

import eu.inn.config.ConfigLoader
import eu.inn.metrics.modules.MetricsModule
import scaldi.Injector

object ModuleAggregator {
  lazy val config = ConfigLoader()

  lazy val injector: Injector = loadConfigInjectedModules(new ServiceModule(config)) :: new MetricsModule

  def loadConfigInjectedModules(previous: Injector): Injector = {
    import scala.collection.JavaConversions._
    if (config.hasPath("inject-modules")) {
      var module = previous
      config.getStringList("inject-modules").foreach { injectModuleClassName ⇒
        module = module :: Class.forName(injectModuleClassName).newInstance().asInstanceOf[Injector]
      }
      module
    } else {
      previous
    }
  }
}
