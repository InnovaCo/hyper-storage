package eu.inn.revault.utils

import eu.inn.metrics.{Metrics, ProcessMetrics}
import eu.inn.metrics.loaders.MetricsReporterLoader
import org.slf4j.LoggerFactory
import scaldi.{Injectable, Injector, TypeTagIdentifier}


object MetricsUtils extends Injectable {
  val log = LoggerFactory.getLogger(getClass)
  def startReporter(metrics: Metrics)(implicit injector: Injector): Unit = {
    import scala.reflect.runtime.universe._
    injector.getBinding(List(TypeTagIdentifier(typeOf[MetricsReporterLoader]))) match {
      case Some(_) ⇒
        inject[MetricsReporterLoader].run()
        ProcessMetrics.startReporting(metrics)

      case None ⇒
        log.warn("Metric reporter is not configured.")
    }
  }
}
