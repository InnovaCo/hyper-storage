import java.util.concurrent.atomic.AtomicInteger

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.event.LoggingReceive
import akka.testkit.{TestProbe, TestKit, TestFSMRef, TestActorRef}
import com.typesafe.config.ConfigFactory
import eu.inn.hyperbus.transport._
import eu.inn.revault._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest._

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}

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
        TestRegistry.processed += self.path.toString → task
      }
      sender() ! ReadyForNextTask
    }
  }
}

object TestRegistry {
  val processed = TrieMap[String, Task]()
}

class TestSingleNode extends FreeSpec with ScalaFutures with Matchers with BeforeAndAfterEach {

  var _actorSystem0: ActorSystem = null

  def actorSystem0 = {
    if (_actorSystem0 == null)
      _actorSystem0 = ActorSystem("eu-inn-cluster", ConfigFactory.load().getConfig("actor-systems.eu-inn-single"))
    _actorSystem0
  }

  override def afterEach {
    println("------- SHUTTING DOWN ACTOR SYSTEM -------- ")
    if (_actorSystem0 != null) TestKit.shutdownActorSystem(actorSystem0)
    _actorSystem0 = null
    Thread.sleep(1000)
  }

  def activate(workerCount: Int = 1) = {
    implicit val as = actorSystem0
    val fsm = TestFSMRef(new ProcessorFSM(Props[TestWorker], workerCount), "revault")
    val processorFsm: TestActorRef[ProcessorFSM] = fsm

    fsm.stateName should equal(RevaultMemberStatus.Activating)
    val t = new TestKit(actorSystem0)
    t.awaitCond {
      fsm.stateName == RevaultMemberStatus.Active
    }
    fsm
  }

  "TestProcessor in a single-node cluster" - {
    "ProcessorFSM should become Active" in {
      activate()
    }

    "ProcessorFSM should shutdown gracefully" in {
      implicit val as = actorSystem0
      val fsm = activate()

      val probe = TestProbe()
      probe watch fsm
      fsm ! ShutdownProcessor
      fsm.stateName should equal(RevaultMemberStatus.Deactivating)
      probe.expectTerminated(fsm)
    }

    "ProcessorFSM should process task" in {
      implicit val as = actorSystem0
      val fsm = activate()
      val task = Task("abc", TestTaskContent("t1"))
      fsm ! task
      val t = new TestKit(actorSystem0)
      t.awaitCond {
        TestRegistry.processed.find(_._2 == task).nonEmpty
      }
    }

    "ProcessorFSM should stash task when workers are busy and process later" in {
      implicit val as = actorSystem0
      val t = new TestKit(actorSystem0)
      val fsm = activate()
      val task1 = Task("abc1", TestTaskContent("t1"))
      val task2 = Task("abc2", TestTaskContent("t2"))
      fsm ! task1
      fsm ! task2
      t.awaitCond {
        TestRegistry.processed.find(_._2 == task1).nonEmpty
      }
      t.awaitCond {
        TestRegistry.processed.find(_._2 == task2).nonEmpty
      }
    }
  }
}
