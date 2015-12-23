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

class TestTwoNode(actorSystem1: ActorSystem, actorSystem2: ActorSystem)
  extends FreeSpec with ScalaFutures with Matchers with BeforeAndAfterAll {

  def this() = this(
    ActorSystem("eu-inn-cluster", ConfigFactory.load().getConfig("actor-systems.eu-inn-1")),
    ActorSystem("eu-inn-cluster", ConfigFactory.load().getConfig("actor-systems.eu-inn-2"))
  )

  override def afterAll {
    TestKit.shutdownActorSystem(actorSystem1)
    TestKit.shutdownActorSystem(actorSystem2)
  }

  "TestProcessor in a two-node cluster" - {
    "ProcessorFSM should become Active" in {
      val fsm1 = {
        implicit val as = actorSystem1
        val fsm1 = TestFSMRef(new ProcessorFSM, "revault")
        val processorFsm1: TestActorRef[ProcessorFSM] = fsm1
        fsm1.stateName should equal(RevaultMemberStatus.Activating)
        fsm1
      }

      val fsm2 = {
        implicit val as = actorSystem2
        val fsm2 = TestFSMRef(new ProcessorFSM, "revault")
        val processorFsm2: TestActorRef[ProcessorFSM] = fsm2
        fsm2.stateName should equal(RevaultMemberStatus.Activating)
        fsm2
      }

      new TestKit(actorSystem1) awaitCond {
        fsm1.stateName == RevaultMemberStatus.Active && fsm1.stateData.members.nonEmpty
      }

      new TestKit(actorSystem2) awaitCond {
        fsm2.stateName == RevaultMemberStatus.Active && fsm2.stateData.members.nonEmpty
      }
    }
  }
}
