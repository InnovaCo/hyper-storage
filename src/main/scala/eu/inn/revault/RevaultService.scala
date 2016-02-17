package eu.inn.revault

import akka.actor.{PoisonPill, Props}
import akka.cluster.Cluster
import akka.util.Timeout
import com.typesafe.config.Config
import eu.inn.hyperbus.HyperBus
import eu.inn.hyperbus.transport.ActorSystemRegistry
import eu.inn.hyperbus.transport.api.{TransportManager, TransportConfigurationLoader}
import eu.inn.revault.db.Db
import eu.inn.revault.sharding.ShardProcessor
import eu.inn.servicecontrol.api.{Service, Console}
import org.slf4j.{LoggerFactory, Logger}
import scaldi.{Injectable, Injector}

import eu.inn.config.ConfigExtenders._
import scala.concurrent.Await
import scala.concurrent.duration._
import eu.inn.hyperbus.akkaservice._
import scala.concurrent.ExecutionContext.Implicits.global // todo: inject

class RevaultService(console: Console, config: Config, implicit val injector: Injector) extends Service with Injectable {
  var log = LoggerFactory.getLogger(getClass)
  log.info(s"Starting Revault service v${BuildInfo.version}...")

  // configuration
  val shutdownTimeout = config.getFiniteDuration("revault.shutdown-timeout")

  // initialize
  log.info(s"Initializing hyperbus...")
  val transportConfiguration = TransportConfigurationLoader.fromConfig(config)
  val transportManager = new TransportManager(transportConfiguration)
  val hyperBus = new HyperBus(transportManager)

  // currently we rely on the name of system
  val actorSystem = ActorSystemRegistry.get("eu-inn").get
  val cluster = Cluster(actorSystem)

  log.info(s"Initializing database connection...")
  val cassandraSession = CassandraConnector.createCassandraSession(config.getConfig("cassandra"), "revault")
  val db = new Db(cassandraSession)

  // worker actor todo: recovery job
  val workerProps = Props(classOf[RevaultWorker], hyperBus, db, actorSystem.deadLetters)
  val workerSettings = Map("revault" → (workerProps, 1)) // todo: configure max worker settings

  // processor actor
  val processorActorRef = actorSystem.actorOf(Props(new ShardProcessor(workerSettings, "revault")))

  val distributor = actorSystem.actorOf(Props(classOf[RevaultDistributor], processorActorRef, db))
  implicit val timeout = Timeout(20.seconds)
  val subscriptions = hyperBus.routeTo[RevaultDistributor](distributor)
  log.info("Started!")

  // shutdown
  override def stopService(controlBreak: Boolean): Unit = {
    log.info("Stopping Revault service...")

    subscriptions.foreach(hyperBus.off)

    try {
      akka.pattern.gracefulStop(processorActorRef, shutdownTimeout*4/5, PoisonPill)
    } catch {
      case t: Throwable ⇒
        log.error("ProcessorActor didn't shutdown gracefully", t)
    }

    try {
      Await.result(hyperBus.shutdown(shutdownTimeout*4/5), shutdownTimeout)
    } catch {
      case t: Throwable ⇒
        log.error("HyperBus didn't shutdown gracefully", t)
    }
    val cluster = cassandraSession.getCluster
    cassandraSession.close()
    cluster.close()
    log.info("Stopped!")
  }
}
