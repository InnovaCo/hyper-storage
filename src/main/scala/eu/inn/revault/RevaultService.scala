package eu.inn.revault

import akka.actor.{PoisonPill, Props}
import akka.cluster.Cluster
import com.typesafe.config.Config
import eu.inn.config.ConfigExtenders._
import eu.inn.hyperbus.Hyperbus
import eu.inn.hyperbus.akkaservice._
import eu.inn.hyperbus.transport.ActorSystemRegistry
import eu.inn.hyperbus.transport.api.{TransportConfigurationLoader, TransportManager}
import eu.inn.revault.db.Db
import eu.inn.revault.recovery.{ShutdownRecoveryWorker, HotRecoveryWorker, StaleRecoveryWorker}
import eu.inn.revault.sharding.{ShardProcessor, SubscribeToShardStatus}
import eu.inn.servicecontrol.api.{Console, Service}
import org.slf4j.LoggerFactory
import scaldi.Injectable

import scala.concurrent.{Await, ExecutionContext}
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
  val completerTimeout = config.getFiniteDuration("revault.completer-timeout")
  val requestTimeout = config.getFiniteDuration("revault.request-timeout")
  val failTimeout = config.getFiniteDuration("revault.fail-timeout")
  val hotRecovery = config.getFiniteDuration("revault.hot-recovery")
  val hotRecoveryRetry = config.getFiniteDuration("revault.hot-recovery-retry")
  val staleRecovery = config.getFiniteDuration("revault.stale-recovery")
  val staleRecoveryRetry = config.getFiniteDuration("revault.stale-recovery-retry")

  // initialize
  log.info(s"Initializing hyperbus...")
  val transportConfiguration = TransportConfigurationLoader.fromConfig(config)
  val transportManager = new TransportManager(transportConfiguration)
  val hyperbus = new Hyperbus(transportManager)

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
  val workerProps = Props(classOf[RevaultWorker], hyperbus, db, completerTimeout)
  val completerProps = Props(classOf[RevaultCompleter], hyperbus, db)
  val workerSettings = Map(
    "revault" → (workerProps, maxWorkers),
    "revault-completer" → (completerProps, maxCompleters)
  )

  // processor actor
  val processorActorRef = actorSystem.actorOf(Props(new ShardProcessor(workerSettings, "revault", shardSyncTimeout)), "revault")

  val distributor = actorSystem.actorOf(Props(classOf[HyperbusAdapter], processorActorRef, db, requestTimeout))

  val subscriptions = Await.result({
    implicit val timeout: akka.util.Timeout = requestTimeout
    hyperbus.routeTo[HyperbusAdapter](distributor)
  }, requestTimeout)

  val hotPeriod = (hotRecovery.toMillis, failTimeout.toMillis)
  log.info(s"Launching hot recovery $hotRecovery-$failTimeout")
  val hotRecoveryActorRef = actorSystem.actorOf(Props(classOf[HotRecoveryWorker], hotPeriod, db, processorActorRef, hotRecoveryRetry, completerTimeout))
  processorActorRef ! SubscribeToShardStatus(hotRecoveryActorRef)

  val stalePeriod = (staleRecovery.toMillis, hotRecovery.toMillis)
  log.info(s"Launching stale recovery $staleRecovery-$hotRecovery")
  val staleRecoveryActorRef = actorSystem.actorOf(Props(classOf[StaleRecoveryWorker], stalePeriod, db, processorActorRef, staleRecoveryRetry, completerTimeout))
  processorActorRef ! SubscribeToShardStatus(staleRecoveryActorRef)

  log.info("Revault started!")

  // shutdown
  override def stopService(controlBreak: Boolean): Unit = {
    log.info("Stopping Revault service...")

    staleRecoveryActorRef ! ShutdownRecoveryWorker
    hotRecoveryActorRef ! ShutdownRecoveryWorker
    subscriptions.foreach(subscription => Await.result(hyperbus.off(subscription), shutdownTimeout/2))

    try {
      akka.pattern.gracefulStop(processorActorRef, shutdownTimeout*4/5, PoisonPill)
    } catch {
      case t: Throwable ⇒
        log.error("ProcessorActor didn't shutdown gracefully", t)
    }

    try {
      Await.result(hyperbus.shutdown(shutdownTimeout*4/5), shutdownTimeout)
    } catch {
      case t: Throwable ⇒
        log.error("Hyperbus didn't shutdown gracefully", t)
    }
    db.close()
    log.info("Revault stopped.")
  }
}
