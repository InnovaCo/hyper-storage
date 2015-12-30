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

class TestTwoNode extends FreeSpec with ScalaFutures with Matchers with BeforeAndAfterEach {

  var _actorSystem1: ActorSystem = null
  var _actorSystem2: ActorSystem = null

  def actorSystem1 = {
    if (_actorSystem1 == null)
      _actorSystem1 = ActorSystem("eu-inn-cluster", ConfigFactory.load().getConfig("actor-systems.eu-inn-1"))
    _actorSystem1
  }

  def actorSystem2 = {
    if (_actorSystem2 == null)
      _actorSystem2 = ActorSystem("eu-inn-cluster", ConfigFactory.load().getConfig("actor-systems.eu-inn-2"))
    _actorSystem2
  }

  override def afterEach {
    println("------- SHUTTING DOWN -------- ")
    if (_actorSystem1 != null) TestKit.shutdownActorSystem(actorSystem1)
    _actorSystem1 = null
    if (_actorSystem2 != null) TestKit.shutdownActorSystem(actorSystem2)
    _actorSystem2 = null
    Thread.sleep(1000)
  }

  "TestProcessor in a two-node cluster" - {
    "ProcessorFSM should become Active" in {
      val fsm1 = {
        implicit val as = actorSystem1
        val fsm1 = TestFSMRef(new ProcessorFSM(Props[TestWorker], 1), "revault")
        val processorFsm1: TestActorRef[ProcessorFSM] = fsm1
        fsm1.stateName should equal(RevaultMemberStatus.Activating)
        fsm1
      }

      val fsm2 = {
        implicit val as = actorSystem2
        val fsm2 = TestFSMRef(new ProcessorFSM(Props[TestWorker], 1), "revault")
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
      {
        implicit val as = actorSystem1
        val probe = TestProbe()
        probe watch fsm1
        fsm1 ! ShutdownProcessor
        fsm1.stateName should equal(RevaultMemberStatus.Deactivating)
        probe.expectTerminated(fsm1, 10 second)
        val c = Cluster(actorSystem1)
        //c.leave(c.selfAddress)
        c.down(c.selfAddress)
        Thread.sleep(1000)
        TestKit.shutdownActorSystem(actorSystem1)
        _actorSystem1 = null
      }
      {
        implicit val as = actorSystem2
        val probe = TestProbe()
        probe watch fsm2
        fsm2 ! ShutdownProcessor
        fsm2.stateName should equal(RevaultMemberStatus.Deactivating)
        probe.expectTerminated(fsm2, 10 second)
      }
    }

    "ProcessorFSM should become Active sequentially" in {
      val fsm1 = {
        implicit val as = actorSystem1
        val fsm1 = TestFSMRef(new ProcessorFSM(Props[TestWorker], 1), "revault")
        val processorFsm1: TestActorRef[ProcessorFSM] = fsm1
        fsm1.stateName should equal(RevaultMemberStatus.Activating)
        fsm1
      }

      new TestKit(actorSystem1) awaitCond {
        fsm1.stateName == RevaultMemberStatus.Active && fsm1.stateData.members.isEmpty
      }

      val fsm2 = {
        implicit val as = actorSystem2
        val fsm2 = TestFSMRef(new ProcessorFSM(Props[TestWorker], 1), "revault")
        val processorFsm2: TestActorRef[ProcessorFSM] = fsm2
        fsm2.stateName should equal(RevaultMemberStatus.Activating)
        fsm2
      }

      new TestKit(actorSystem2) awaitCond ({
        fsm2.stateName == RevaultMemberStatus.Active && fsm2.stateData.members.nonEmpty
      }, 5 second)

      {
        implicit val as = actorSystem1
        val probe = TestProbe()
        probe watch fsm1
        fsm1 ! ShutdownProcessor
        fsm1.stateName should equal(RevaultMemberStatus.Deactivating)
        probe.expectTerminated(fsm1)
        val c = Cluster(actorSystem1)
        //c.leave(c.selfAddress)
        c.down(c.selfAddress)
        Thread.sleep(2000)
        TestKit.shutdownActorSystem(actorSystem1)
        _actorSystem1 = null
      }

      new TestKit(actorSystem2) awaitCond ({
        fsm2.stateName == RevaultMemberStatus.Active && fsm2.stateData.members.isEmpty
      }, 10 second)

      {
        implicit val as = actorSystem2
        val probe = TestProbe()
        probe watch fsm2
        fsm2 ! ShutdownProcessor
        fsm2.stateName should equal(RevaultMemberStatus.Deactivating)
        probe.expectTerminated(fsm2, 10 second)
      }
    }
  }
}
