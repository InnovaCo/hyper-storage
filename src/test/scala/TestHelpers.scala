import java.util.{Date, UUID}

import akka.actor._
import akka.cluster.Cluster
import akka.testkit._
import com.datastax.driver.core.utils.UUIDs
import com.typesafe.config.ConfigFactory
import eu.inn.hyperbus.HyperBus
import eu.inn.hyperbus.transport.ActorSystemRegistry
import eu.inn.hyperbus.transport.api.{TransportManager, TransportConfigurationLoader}
import eu.inn.revault.MonitorLogic
import eu.inn.revault.db.{Db, Monitor}
import eu.inn.revault.sharding._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterEach, Matchers}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext.Implicits.global

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

trait TestHelpers extends Matchers with BeforeAndAfterEach with ScalaFutures {
  this : org.scalatest.BeforeAndAfterEach with org.scalatest.Suite =>

  def createRevaultActor(groupName: String, workerCount: Int = 1, waitWhileActivates: Boolean = true)(implicit actorSystem: ActorSystem) = {
    val workerSettings = Map(groupName → (Props[TestWorker], workerCount))
    val fsm = new TestFSMRef[ShardMemberStatus, ShardedClusterData, ShardProcessor](actorSystem,
      Props(new ShardProcessor(workerSettings, "revault")).withDispatcher("deque-dispatcher"),
      GuardianExtractor.guardian(actorSystem),
      "revault"
    )
    //val fsm = TestFSMRef(new ProcessorFSM(Props[TestWorker], workerCount), "revault")
    //val processorFsm: TestActorRef[ProcessorFSM] = fsm
    fsm.stateName should equal(ShardMemberStatus.Activating)
    if (waitWhileActivates) {
      val t = new TestKit(actorSystem)
      t.awaitCond(fsm.stateName == ShardMemberStatus.Active)
    }
    fsm
  }

  //val _actorSystems = TrieMap[Int, ActorSystem]()
  val _hyperBuses = TrieMap[Int, HyperBus]()

  def testActorSystem(index: Int = 0) = {
    testHyperBus(index)
    ActorSystemRegistry.get(s"eu-inn-$index").get
  }

  def testKit(index: Int = 0) = new TestKit(testActorSystem(index)) with ImplicitSender

  def testHyperBus(index: Int = 0) = {
    val hb = _hyperBuses.getOrElseUpdate ( index, {
        val config = ConfigFactory.load().getConfig(s"hyperbus-$index")
        val transportConfiguration = TransportConfigurationLoader.fromConfig(config)
        val transportManager = new TransportManager(transportConfiguration)
        new HyperBus(transportManager)
      }
    )
    hb
  }

  def shutdownCluster(index: Int = 0): Unit = {
    _hyperBuses.get(index).foreach { hb ⇒
      hb.shutdown(5.seconds)
      Thread.sleep(1000)
    }
  }

  def selectMonitors(uuids: Seq[UUID], path: String, db: Db): Seq[Monitor] = {
    uuids flatMap { uuid ⇒
      val monitorChannel = MonitorLogic.channelFromUri(path)
      val qt = MonitorLogic.getDtQuantum(UUIDs.unixTimestamp(uuid))
      whenReady(db.selectMonitor(qt, monitorChannel, path, uuid)) { mon ⇒
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
    println("------- SHUTTING DOWN HYPERBUSES -------- ")
    _hyperBuses.foreach{
      case (index, hb) ⇒ {
        Await.result(hb.shutdown(10.second), 11.second)
      }
    }
    _hyperBuses.clear()
    Thread.sleep(500)
  }

  implicit class TaskEx(t: ShardTask) {
    def isProcessingStarted = t.asInstanceOf[TestShardTask].isProcessingStarted
    def isProcessed = t.asInstanceOf[TestShardTask].isProcessed
    def processorPath = t.asInstanceOf[TestShardTask].processActorPath getOrElse ""
  }
}
