import akka.actor.{ActorLogging, Actor, ActorSystem, Props}
import akka.testkit.{TestKit, TestActorRef, TestFSMRef}
import akka.text.GuardianExtractor
import com.typesafe.config.ConfigFactory
import eu.inn.revault._
import org.scalatest.{BeforeAndAfterEach, Matchers}

import scala.collection.concurrent.TrieMap

case class TestTaskContent(value: String, ttl: Long = System.currentTimeMillis()+10*1000) extends Expireable {
  def isExpired = ttl < System.currentTimeMillis()
}

class TestWorker extends Actor with ActorLogging {
  def receive = {
    case task @ Task(key, content: TestTaskContent) => {
      if (content.isExpired) {
        log.error(s"Task content is expired: $task")
      } else {
        log.info(s"Processed task: $task")
        TestRegistry.processed += (self.path.toString + ":" + task.key) → task
      }
      sender() ! ReadyForNextTask
    }
  }
}

object TestRegistry {
  val processed = TrieMap[String, Task]()
}

trait TestHelpers extends Matchers with BeforeAndAfterEach {
  this : org.scalatest.BeforeAndAfterEach with org.scalatest.Suite =>

  def createRevaultActor(workerCount: Int = 1)(implicit actorSystem: ActorSystem) = {
    val fsm = new TestFSMRef[RevaultMemberStatus, ProcessorData, ProcessorFSM](actorSystem,
      Props(new ProcessorFSM(Props[TestWorker], workerCount)).withDispatcher("deque-dispatcher"),
      GuardianExtractor.guardian(actorSystem),
      "revault"
    )
    //val fsm = TestFSMRef(new ProcessorFSM(Props[TestWorker], workerCount), "revault")
    val processorFsm: TestActorRef[ProcessorFSM] = fsm

    fsm.stateName should equal(RevaultMemberStatus.Activating)
    val t = new TestKit(actorSystem)
    t.awaitCond {
      fsm.stateName == RevaultMemberStatus.Active
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

  override def afterEach() {
    println("------- SHUTTING DOWN ACTOR SYSTEMS -------- ")
    _actorSystems.foreach{
      case (_, as) ⇒ TestKit.shutdownActorSystem(as)
    }
    _actorSystems.clear()
    Thread.sleep(500)
  }
}
