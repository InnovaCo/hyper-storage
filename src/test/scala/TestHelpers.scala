import java.util.UUID

import akka.actor._
import akka.cluster.Cluster
import akka.testkit._
import com.codahale.metrics.ScheduledReporter
import com.datastax.driver.core.utils.UUIDs
import com.typesafe.config.ConfigFactory
import eu.inn.hyperbus.Hyperbus
import eu.inn.hyperbus.transport.ActorSystemRegistry
import eu.inn.hyperbus.transport.api.{TransportConfigurationLoader, TransportManager}
import eu.inn.metrics.MetricsTracker
import eu.inn.metrics.modules.ConsoleReporterModule
import eu.inn.revault.TransactionLogic
import eu.inn.revault.db.{Db, Transaction}
import eu.inn.revault.sharding._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterEach, Matchers}
import org.slf4j.LoggerFactory
import scaldi.Injectable

import scala.collection.concurrent.TrieMap
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

case class TestShardTask(key: String, value: String,
                         sleep: Int = 0,
                         ttl: Long = System.currentTimeMillis()+60*1000,
                         id: UUID = UUID.randomUUID()) extends ShardTask {
  def group = "test-group"
  def isExpired = ttl < System.currentTimeMillis()
  def processingStarted(actorPath: String): Unit = {
    ProcessedRegistry.tasksStarted += id → (actorPath, this)
  }
  def processed(actorPath: String): Unit = {
    ProcessedRegistry.tasks += id → (actorPath, this)
  }
  def isProcessed = ProcessedRegistry.tasks.get(id).isDefined
  def isProcessingStarted = ProcessedRegistry.tasksStarted.get(id).isDefined
  def processActorPath: Option[String] = ProcessedRegistry.tasks.get(id) map { kv ⇒ kv._1 }
  override def toString = s"TestTask($key, $value, $sleep, $ttl, #${System.identityHashCode(this)}, actor: $processActorPath"
}

object ProcessedRegistry {
  val tasks = TrieMap[UUID, (String, TestShardTask)]()
  val tasksStarted = TrieMap[UUID, (String, TestShardTask)]()
}

class TestWorker extends Actor with ActorLogging {
  def receive = {
    case task: TestShardTask => {
      if (task.isExpired) {
        log.error(s"Task is expired: $task")
      } else {
        val c = Cluster(context.system)
        val path = c.selfAddress + "/" + self.path.toString
        log.info(s"Processing task: $task")
        task.processingStarted(path)
        if (task.sleep > 0 ) {
          Thread.sleep(task.sleep)
        }
        task.processed(path)
        log.info(s"Task processed: $task")
      }
      sender() ! ShardTaskComplete(task, None)
    }
  }
}

trait TestHelpers extends Matchers with BeforeAndAfterEach with ScalaFutures with Injectable  {
  this : org.scalatest.BeforeAndAfterEach with org.scalatest.Suite =>
  private [this] val log = LoggerFactory.getLogger(getClass)
  implicit val injector = new ConsoleReporterModule(Duration.Inf)
  val tracker = inject[MetricsTracker]
  val reporter = inject[ScheduledReporter]

  implicit def executionContext: ExecutionContext = {
    scala.concurrent.ExecutionContext.Implicits.global
  }

  def createRevaultActor(groupName: String, workerCount: Int = 1, waitWhileActivates: Boolean = true)(implicit actorSystem: ActorSystem) = {
    val workerSettings = Map(groupName → (Props[TestWorker], workerCount))
    val fsm = new TestFSMRef[ShardMemberStatus, ShardedClusterData, ShardProcessor](actorSystem,
      ShardProcessor.props(workerSettings, "revault", tracker).withDispatcher("deque-dispatcher"),
      GuardianExtractor.guardian(actorSystem),
      "revault"
    )
    //val fsm = TestFSMRef(new ShardProcessor(Props[TestWorker], workerCount), "revault")
    //val ShardProcessor: TestActorRef[ShardProcessor] = fsm
    fsm.stateName should equal(ShardMemberStatus.Activating)
    if (waitWhileActivates) {
      val t = new TestKit(actorSystem)
      t.awaitCond(fsm.stateName == ShardMemberStatus.Active)
    }
    fsm
  }

  //val _actorSystems = TrieMap[Int, ActorSystem]()
  val _hyperbuses = TrieMap[Int, Hyperbus]()

  def testActorSystem(index: Int = 0) = {
    testHyperbus(index)
    ActorSystemRegistry.get(s"eu-inn-$index").get
  }

  def testKit(index: Int = 0) = new TestKit(testActorSystem(index)) with ImplicitSender

  def testHyperbus(index: Int = 0) = {
    val hb = _hyperbuses.getOrElseUpdate ( index, {
        val config = ConfigFactory.load().getConfig(s"hyperbus-$index")
        val transportConfiguration = TransportConfigurationLoader.fromConfig(config)
        val transportManager = new TransportManager(transportConfiguration)
        new Hyperbus(transportManager, defaultGroupName = Some(s"subscriber-$index"), logMessages = true)
      }
    )
    hb
  }

  def shutdownCluster(index: Int = 0): Unit = {
    _hyperbuses.get(index).foreach { hb ⇒
      hb.shutdown(5.seconds)
      Thread.sleep(1000)
    }
  }

  def selectTransactions(uuids: Seq[UUID], path: String, db: Db): Seq[Transaction] = {
    uuids flatMap { uuid ⇒
      val partition = TransactionLogic.partitionFromUri(path)
      val qt = TransactionLogic.getDtQuantum(UUIDs.unixTimestamp(uuid))
      whenReady(db.selectTransaction(qt, partition, path, uuid)) { mon ⇒
        mon
      }
    }
  }

  def shutdownRevaultActor(fsm: TestFSMRef[ShardMemberStatus, ShardedClusterData, ShardProcessor])(implicit actorSystem: ActorSystem) = {
    val probe = TestProbe()
    probe watch fsm
    fsm ! ShutdownProcessor
    new TestKit(actorSystem).awaitCond(fsm.stateName == ShardMemberStatus.Deactivating, 10.second)
    probe.expectTerminated(fsm, 10.second)
  }

  override def afterEach() {
    log.info("------- SHUTTING DOWN HYPERBUSES -------- ")
    _hyperbuses.foreach{
      case (index, hb) ⇒ {
        Await.result(hb.shutdown(10.second), 11.second)
      }
    }
    _hyperbuses.clear()
    Thread.sleep(500)
    log.info("------- HYPERBUSES WERE SHUT DOWN -------- ")
    reporter.report()
  }

  implicit class TaskEx(t: ShardTask) {
    def isProcessingStarted = t.asInstanceOf[TestShardTask].isProcessingStarted
    def isProcessed = t.asInstanceOf[TestShardTask].isProcessed
    def processorPath = t.asInstanceOf[TestShardTask].processActorPath getOrElse ""
  }
}
