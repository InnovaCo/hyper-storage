package eu.inn.revault

import akka.actor.{PoisonPill, Props}
import akka.cluster.Cluster
import akka.util.Timeout
import com.typesafe.config.Config
import eu.inn.config.ConfigExtenders._
import eu.inn.hyperbus.HyperBus
import eu.inn.hyperbus.akkaservice._
import eu.inn.hyperbus.transport.ActorSystemRegistry
import eu.inn.hyperbus.transport.api.{TransportConfigurationLoader, TransportManager}
import eu.inn.revault.db.Db
import eu.inn.revault.sharding.ShardProcessor
import eu.inn.servicecontrol.api.{Console, Service}
import org.slf4j.LoggerFactory
import scaldi.Injectable

import scala.concurrent.{ExecutionContext, Await}
import scala.concurrent.duration._
import scala.util.control.NonFatal

class RevaultService(console: Console,
                     config: Config,
                     connector: CassandraConnector,
                     implicit val ec: ExecutionContext) extends Service with Injectable {
  var log = LoggerFactory.getLogger(getClass)
  log.info(s"Starting Revault service v${BuildInfo.version}...")

  // configuration
  val shutdownTimeout = config.getFiniteDuration("revault.shutdown-timeout")
  val shardSyncTimeout = config.getFiniteDuration("revault.shards-sync-time")
  val maxWorkers = config.getInt("revault.max-workers")
  val maxCompleters = config.getInt("revault.max-completers")
  val completerTaskTtl = config.getLong("revault.completer-task-ttl")
  val requestTimeout = config.getFiniteDuration("revault.request-timeout")

  // initialize
  log.info(s"Initializing hyperbus...")
  val transportConfiguration = TransportConfigurationLoader.fromConfig(config)
  val transportManager = new TransportManager(transportConfiguration)
  val hyperBus = new HyperBus(transportManager)

  // currently we rely on the name of system
  val actorSystem = ActorSystemRegistry.get("eu-inn").get
  val cluster = Cluster(actorSystem)

  //
  val db = new Db(connector)
  // trigger connect to c* but continue initialization
  try {
    log.info(s"Initializing database connection...")
    db.preStart()
  } catch {
    case NonFatal(e) ⇒
      log.error(s"Can't create C* session", e)
  }

  // worker actor todo: recovery job
  val workerProps = Props(classOf[RevaultWorker], hyperBus, db, completerTaskTtl)
  val completerProps = Props(classOf[RevaultCompleter], hyperBus, db)
  val workerSettings = Map(
    "revault" → (workerProps, maxWorkers),
    "revault-completer" → (completerProps, maxCompleters)
  )

  // processor actor
  val processorActorRef = actorSystem.actorOf(Props(new ShardProcessor(workerSettings, "revault", shardSyncTimeout)))

  val distributor = actorSystem.actorOf(Props(classOf[RevaultDistributor], processorActorRef, db, requestTimeout))

  val subscriptions = {
    implicit val timeout: akka.util.Timeout = requestTimeout
    hyperBus.routeTo[RevaultDistributor](distributor)
  }

  log.info("Revault started!")

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
    db.close()
    log.info("Revault stopped.")
  }
}
