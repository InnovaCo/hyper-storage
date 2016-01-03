import java.util.UUID

import akka.actor._
import akka.cluster.Cluster
import akka.testkit.{TestProbe, TestKit, TestActorRef, TestFSMRef}
import akka.text.GuardianExtractor
import com.typesafe.config.ConfigFactory
import eu.inn.revault._
import org.scalatest.{BeforeAndAfterEach, Matchers}
import scala.concurrent.duration._
import scala.collection.concurrent.TrieMap

case class TestTaskContent(value: String,
                           sleep: Int = 0,
                           ttl: Long = System.currentTimeMillis()+60*1000,
                           id: UUID = UUID.randomUUID()) extends Expireable {
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
  override def toString = s"TestTaskContent($value, $sleep, $ttl, #${System.identityHashCode(this)}, actor: $processActorPath"
}

object ProcessedRegistry {
  val tasks = TrieMap[UUID, (String, TestTaskContent)]()
  val tasksStarted = TrieMap[UUID, (String, TestTaskContent)]()
}

class TestWorker extends Actor with ActorLogging {
  def receive = {
    case task @ Task(key, content: TestTaskContent) => {
      if (content.isExpired) {
        log.error(s"Task content is expired: $task")
      } else {
        val c = Cluster(context.system)
        val path = c.selfAddress + "/" + self.path.toString
        log.info(s"Processing task: $task")
        content.processingStarted(path)
        if (content.sleep > 0 ) {
          Thread.sleep(content.sleep)
        }
        content.processed(path)
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

  val _actorSystems = TrieMap[Int, ActorSystem]()

  def testActorSystem(index: Int = 0) = {
    val as = _actorSystems.getOrElseUpdate ( index,
      ActorSystem("eu-inn-cluster", ConfigFactory.load().getConfig(s"actor-systems.eu-inn-$index"))
    )
    (as, new TestKit(as))
  }

  def shutdownActorSystem(index: Int = 0): Unit = {
    _actorSystems.get(index).foreach { as ⇒
      TestKit.shutdownActorSystem(as)
      _actorSystems.remove(index)
    }
  }

  def shutdownCluster(index: Int = 0): Unit = {
    _actorSystems.get(index).map { as ⇒
      val c = Cluster(as)
      c.down(c.selfAddress)
      Thread.sleep(1000)
    } getOrElse {
      fail(s"There is no actor system #$index")
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
    println("------- SHUTTING DOWN ACTOR SYSTEMS -------- ")
    _actorSystems.foreach{
      case (_, as) ⇒ TestKit.shutdownActorSystem(as)
    }
    _actorSystems.clear()
    Thread.sleep(500)
  }

  implicit class TaskEx(t: Task) {
    def isProcessingStarted = t.content.asInstanceOf[TestTaskContent].isProcessingStarted
    def isProcessed = t.content.asInstanceOf[TestTaskContent].isProcessed
    def processorPath = t.content.asInstanceOf[TestTaskContent].processActorPath getOrElse ""
  }
}
