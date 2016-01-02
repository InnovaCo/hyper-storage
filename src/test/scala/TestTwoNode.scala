import java.util.concurrent.atomic.AtomicInteger

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.event.LoggingReceive
import akka.testkit.{TestProbe, TestKit, TestFSMRef, TestActorRef}
import com.typesafe.config.ConfigFactory
import eu.inn.hyperbus.transport._
import eu.inn.revault.{ShutdownProcessor, RevaultMemberStatus, ProcessorFSM}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}

class TestTwoNode extends FreeSpec with ScalaFutures with TestHelpers {
  "TestProcessor in a two-node cluster" - {
    "ProcessorFSM should become Active" in {
      val (fsm1, actorSystem1, testKit1) = {
        implicit val (actorSystem1, testKit1) = testActorSystem(1)
        val fsm1 = createRevaultActor()
        (fsm1, actorSystem1, testKit1)
      }

      val (fsm2, actorSystem2, testKit2) = {
        implicit val (actorSystem2, testKit2) = testActorSystem(2)
        val fsm2 = createRevaultActor()
        (fsm2, actorSystem2, testKit2)
      }

      testKit1.awaitCond(fsm1.stateName == RevaultMemberStatus.Active && fsm1.stateData.members.nonEmpty)

      testKit2.awaitCond(fsm2.stateName == RevaultMemberStatus.Active && fsm2.stateData.members.nonEmpty)

      {
        implicit val as = actorSystem1
        val probe = TestProbe()
        probe watch fsm1
        fsm1 ! ShutdownProcessor
        testKit1.awaitCond(fsm1.stateName == RevaultMemberStatus.Deactivating)
        probe.expectTerminated(fsm1, 10 second)
        val c = Cluster(actorSystem1)
        //c.leave(c.selfAddress)
        c.down(c.selfAddress)
        Thread.sleep(1000)
        shutdownActorSystem(1)
      }

      {
        implicit val as = actorSystem2
        val probe = TestProbe()
        probe watch fsm2
        fsm2 ! ShutdownProcessor
        testKit2.awaitCond(fsm2.stateName == RevaultMemberStatus.Deactivating)
        probe.expectTerminated(fsm2, 10 second)
      }
    }

    "ProcessorFSM should become Active sequentially" in {
      val (fsm1, actorSystem1, testKit1) = {
        implicit val (actorSystem1, testKit1) = testActorSystem(1)
        val fsm1 = createRevaultActor()
        (fsm1, actorSystem1, testKit1)
      }

      testKit1.awaitCond(fsm1.stateName == RevaultMemberStatus.Active && fsm1.stateData.members.isEmpty)

      val (fsm2, actorSystem2, testKit2) = {
        implicit val (actorSystem2, testKit2) = testActorSystem(2)
        val fsm2 = createRevaultActor()
        (fsm2, actorSystem2, testKit2)
      }

      testKit2.awaitCond(fsm2.stateName == RevaultMemberStatus.Active && fsm2.stateData.members.nonEmpty, 5 second)

      {
        implicit val as = actorSystem1
        val probe = TestProbe()
        probe watch fsm1
        fsm1 ! ShutdownProcessor
        testKit1.awaitCond(fsm1.stateName == RevaultMemberStatus.Deactivating)
        probe.expectTerminated(fsm1)
        val c = Cluster(actorSystem1)
        //c.leave(c.selfAddress)
        c.down(c.selfAddress)
        Thread.sleep(2000)
        shutdownActorSystem(1)
      }

      testKit2.awaitCond(fsm2.stateName == RevaultMemberStatus.Active && fsm2.stateData.members.isEmpty, 10 second)

      {
        implicit val as = actorSystem2
        val probe = TestProbe()
        probe watch fsm2
        fsm2 ! ShutdownProcessor
        testKit2.awaitCond(fsm2.stateName == RevaultMemberStatus.Deactivating)
        probe.expectTerminated(fsm2, 10 second)
      }
    }
  }
}
