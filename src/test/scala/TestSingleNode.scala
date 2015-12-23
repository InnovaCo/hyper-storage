import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorSystem, Actor, ActorLogging}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.event.LoggingReceive
import akka.testkit.{TestKit, TestFSMRef, TestActorRef}
import com.typesafe.config.ConfigFactory
import eu.inn.hyperbus.transport._
import eu.inn.revault.{RevaultMemberStatus, ProcessorFSM}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}

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
      val fsm = TestFSMRef(new ProcessorFSM, "revault")
      val processorFsm: TestActorRef[ProcessorFSM] = fsm

      fsm.stateName should equal(RevaultMemberStatus.Activating)
      val t = new TestKit(actorSystem0)
      t.awaitCond {
        fsm.stateName == RevaultMemberStatus.Active
      }
    }
  }
}
