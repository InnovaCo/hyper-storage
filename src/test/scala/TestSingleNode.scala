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

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}

case class TestTaskContent(value: String, ttl: Long) extends Expireable {
  def isExpired = ttl < System.currentTimeMillis()
}

class TestWorker extends Actor with ActorLogging {
  def receive = {
    case task @ Task(key, content: TestTaskContent) => {
      if (content.isExpired) {
        log.error(s"Task content is expired: $task")
      } else {
        log.info(s"Processed task: $task")
      }
      sender() ! ReadyForNextTask
    }
  }
}

class TestSingleNode(actorSystem0: ActorSystem)
  extends FreeSpec with ScalaFutures with Matchers with BeforeAndAfterAll {

  def this() = this(
    ActorSystem("eu-inn-cluster", ConfigFactory.load().getConfig("actor-systems.eu-inn-single"))
  )

  val cluster = Cluster(actorSystem0)

  override def afterAll {
    TestKit.shutdownActorSystem(actorSystem0)
  }

  "TestProcessor in a single-node cluster" - {
    "ProcessorFSM should become Active" in {
      implicit val as = actorSystem0
      val fsm = TestFSMRef(new ProcessorFSM(Props[TestWorker], 1), "revault")
      val processorFsm: TestActorRef[ProcessorFSM] = fsm

      fsm.stateName should equal(RevaultMemberStatus.Activating)
      val t = new TestKit(actorSystem0)
      t.awaitCond {
        fsm.stateName == RevaultMemberStatus.Active
      }

      val probe = TestProbe()
      probe watch fsm
      fsm ! ShutdownProcessor
      fsm.stateName should equal(RevaultMemberStatus.Deactivating)
      probe.expectTerminated(fsm)
    }
  }
}
