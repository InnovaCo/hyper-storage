package eu.inn.revault.metrics

import eu.inn.metrics.loaders.MetricsReporterLoader
import eu.inn.metrics.{MetricsTracker, ProcessMetrics}
import org.slf4j.LoggerFactory
import scaldi.{Injectable, Injector, TypeTagIdentifier}


object MetricsReporter extends Injectable {
  val log = LoggerFactory.getLogger(getClass)
  def startReporter(tracker: MetricsTracker)(implicit injector: Injector): Unit = {
    import scala.reflect.runtime.universe._
    injector.getBinding(List(TypeTagIdentifier(typeOf[MetricsReporterLoader]))) match {
      case Some(_) ⇒
        inject[MetricsReporterLoader].run()
        ProcessMetrics.startReporting(tracker)

      case None ⇒
        log.warn("Metric reporter is not configured.")
    }
  }
}
