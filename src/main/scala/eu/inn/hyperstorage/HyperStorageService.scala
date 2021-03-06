package eu.inn.hyperstorage

import akka.cluster.Cluster
import com.typesafe.config.Config
import eu.inn.hyperbus.Hyperbus
import eu.inn.hyperbus.akkaservice._
import eu.inn.hyperbus.transport.ActorSystemRegistry
import eu.inn.hyperbus.transport.api.{TransportConfigurationLoader, TransportManager}
import eu.inn.metrics.MetricsTracker
import eu.inn.hyperstorage.db.Db
import eu.inn.hyperstorage.indexing.IndexManager
import eu.inn.hyperstorage.metrics.MetricsReporter
import eu.inn.hyperstorage.recovery.{HotRecoveryWorker, ShutdownRecoveryWorker, StaleRecoveryWorker}
import eu.inn.hyperstorage.sharding.{ShardProcessor, ShutdownProcessor, SubscribeToShardStatus}
import eu.inn.hyperstorage.workers.primary.PrimaryWorker
import eu.inn.hyperstorage.workers.secondary.SecondaryWorker
import eu.inn.servicecontrol.api.{Console, Service}
import org.slf4j.LoggerFactory
import scaldi.{Injectable, Injector}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext}
import scala.util.control.NonFatal

case class HyperStorageConfig(
                               shutdownTimeout: FiniteDuration,
                               shardSyncTimeout: FiniteDuration,
                               maxWorkers: Int,
                               backgroundTaskTimeout: FiniteDuration,
                               requestTimeout: FiniteDuration,
                               failTimeout: FiniteDuration,
                               hotRecovery: FiniteDuration,
                               hotRecoveryRetry: FiniteDuration,
                               staleRecovery: FiniteDuration,
                               staleRecoveryRetry: FiniteDuration
                        )

class HyperStorageService(console: Console,
                          config: Config,
                          connector: CassandraConnector,
                          implicit val ec: ExecutionContext,
                          implicit val injector: Injector) extends Service with Injectable {
  var log = LoggerFactory.getLogger(getClass)
  log.info(s"Starting HyperStorage service v${BuildInfo.version}...")

  // configuration
  import eu.inn.binders.tconfig._
  val serviceConfig = config.getValue("hyper-storage").read[HyperStorageConfig]

  log.info(s"HyperStorage configuration: $config")

  // metrics tracker
  val tracker = inject[MetricsTracker]
  MetricsReporter.startReporter(tracker)

  import serviceConfig._

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

  val indexManagerProps = IndexManager.props(hyperbus, db, tracker, maxWorkers)
  val indexManagerRef = actorSystem.actorOf(
    indexManagerProps, "index-manager"
  )

  // worker actor todo: recovery job
  val primaryWorkerProps = PrimaryWorker.props(hyperbus, db, tracker, backgroundTaskTimeout)
  val secondaryWorkerProps = SecondaryWorker.props(hyperbus, db, tracker, indexManagerRef)
  val workerSettings = Map(
    "hyper-storage-primary-worker" → (primaryWorkerProps, maxWorkers, "pgw-"),
    "hyper-storage-secondary-worker" → (secondaryWorkerProps, maxWorkers, "sgw-")
  )

  // shard processor actor
  val shardProcessorRef = actorSystem.actorOf(
    ShardProcessor.props(workerSettings, "hyper-storage", tracker, shardSyncTimeout), "hyper-storage"
  )

  val adapterRef = actorSystem.actorOf(HyperbusAdapter.props(shardProcessorRef, db, tracker, requestTimeout), "adapter")

  val subscriptions = Await.result({
    implicit val timeout: akka.util.Timeout = requestTimeout
    hyperbus.routeTo[HyperbusAdapter](adapterRef)
  }, requestTimeout)

  val hotPeriod = (hotRecovery.toMillis, failTimeout.toMillis)
  log.info(s"Launching hot recovery $hotRecovery-$failTimeout")
  val hotRecoveryRef = actorSystem.actorOf(HotRecoveryWorker.props(hotPeriod, db, shardProcessorRef, tracker, hotRecoveryRetry, backgroundTaskTimeout), "hot-recovery")
  shardProcessorRef ! SubscribeToShardStatus(hotRecoveryRef)

  val stalePeriod = (staleRecovery.toMillis, hotRecovery.toMillis)
  log.info(s"Launching stale recovery $staleRecovery-$hotRecovery")
  val staleRecoveryRef = actorSystem.actorOf(StaleRecoveryWorker.props(stalePeriod, db, shardProcessorRef, tracker, staleRecoveryRetry, backgroundTaskTimeout), "stale-recovery")
  shardProcessorRef ! SubscribeToShardStatus(staleRecoveryRef)

  log.info(s"Launching index manager")
  shardProcessorRef ! SubscribeToShardStatus(indexManagerRef)

  log.info("HyperStorage started!")

  // shutdown
  override def stopService(controlBreak: Boolean): Unit = {
    log.info("Stopping HyperStorage service...")

    staleRecoveryRef ! ShutdownRecoveryWorker
    hotRecoveryRef ! ShutdownRecoveryWorker
    subscriptions.foreach(subscription => Await.result(hyperbus.off(subscription), shutdownTimeout/2))

    log.info("Stopping processor actor...")
    try {
      akka.pattern.gracefulStop(shardProcessorRef, shutdownTimeout*4/5, ShutdownProcessor)
    } catch {
      case t: Throwable ⇒
        log.error("ProcessorActor didn't stopped gracefully", t)
    }

    try {
      Await.result(hyperbus.shutdown(shutdownTimeout*4/5), shutdownTimeout)
    } catch {
      case t: Throwable ⇒
        log.error("Hyperbus didn't shutdown gracefully", t)
    }
    db.close()
    log.info("HyperStorage stopped.")
  }
}
