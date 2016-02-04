import java.util.{Date, UUID}

import akka.actor._
import akka.cluster.Cluster
import akka.testkit._
import akka.text.GuardianExtractor
import com.typesafe.config.ConfigFactory
import eu.inn.hyperbus.HyperBus
import eu.inn.hyperbus.transport.ActorSystemRegistry
import eu.inn.hyperbus.transport.api.{TransportManager, TransportConfigurationLoader}
import eu.inn.revault._
import org.scalatest.{BeforeAndAfterEach, Matchers}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext.Implicits.global

case class TestTask(key: String, value: String,
                           sleep: Int = 0,
                           ttl: Long = System.currentTimeMillis()+60*1000,
                           id: UUID = UUID.randomUUID()) extends Task {
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
  val tasks = TrieMap[UUID, (String, TestTask)]()
  val tasksStarted = TrieMap[UUID, (String, TestTask)]()
}

class TestWorker extends Actor with ActorLogging {
  def receive = {
    case task: TestTask => {
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
      sender() ! ReadyForNextTask
    }
  }
}

trait TestHelpers extends Matchers with BeforeAndAfterEach {
  this : org.scalatest.BeforeAndAfterEach with org.scalatest.Suite =>

  def createRevaultActor(workerCount: Int = 1, waitWhileActivates: Boolean = true)(implicit actorSystem: ActorSystem) = {
    val fsm = new TestFSMRef[RevaultMemberStatus, ProcessorData, ProcessorFSM](actorSystem,
      Props(new ProcessorFSM(Props[TestWorker], workerCount)).withDispatcher("deque-dispatcher"),
      GuardianExtractor.guardian(actorSystem),
      "revault"
    )
    //val fsm = TestFSMRef(new ProcessorFSM(Props[TestWorker], workerCount), "revault")
    //val processorFsm: TestActorRef[ProcessorFSM] = fsm
    fsm.stateName should equal(RevaultMemberStatus.Activating)
    if (waitWhileActivates) {
      val t = new TestKit(actorSystem)
      t.awaitCond(fsm.stateName == RevaultMemberStatus.Active)
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


  def shutdownRevaultActor(fsm: TestFSMRef[RevaultMemberStatus, ProcessorData, ProcessorFSM])(implicit actorSystem: ActorSystem) = {
    val probe = TestProbe()
    probe watch fsm
    fsm ! ShutdownProcessor
    new TestKit(actorSystem).awaitCond(fsm.stateName == RevaultMemberStatus.Deactivating, 10.second)
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

  implicit class TaskEx(t: Task) {
    def isProcessingStarted = t.asInstanceOf[TestTask].isProcessingStarted
    def isProcessed = t.asInstanceOf[TestTask].isProcessed
    def processorPath = t.asInstanceOf[TestTask].processActorPath getOrElse ""
  }
}
