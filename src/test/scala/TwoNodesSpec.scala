import akka.cluster.Cluster
import akka.testkit.{TestKit, TestProbe}
import eu.inn.revault.{ShutdownProcessor, Task, RevaultMemberStatus}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

class TwoNodesSpec extends FreeSpec with ScalaFutures with TestHelpers {
  "Processor in a two-node cluster" - {
    "ProcessorFSM should become Active" in {
      val (fsm1, actorSystem1, testKit1) = {
        implicit val (actorSystem1, testKit1) = testActorSystem(1)
        (createRevaultActor(waitWhileActivates = false), actorSystem1, testKit1)
      }

      val (fsm2, actorSystem2, testKit2) = {
        implicit val (actorSystem2, testKit2) = testActorSystem(2)
        (createRevaultActor(waitWhileActivates = false), actorSystem2, testKit2)
      }

      testKit1.awaitCond(fsm1.stateName == RevaultMemberStatus.Active && fsm1.stateData.members.nonEmpty)
      testKit2.awaitCond(fsm2.stateName == RevaultMemberStatus.Active && fsm2.stateData.members.nonEmpty)

      shutdownRevaultActor(fsm1)(actorSystem1)
      shutdownCluster(1)
      Thread.sleep(1000)
      shutdownRevaultActor(fsm2)(actorSystem2)
    }

    "ProcessorFSM should become Active sequentially" in {
      val (fsm1, actorSystem1, testKit1) = {
        implicit val (actorSystem1, testKit1) = testActorSystem(1)
        (createRevaultActor(waitWhileActivates = false), actorSystem1, testKit1)
      }

      testKit1.awaitCond(fsm1.stateName == RevaultMemberStatus.Active && fsm1.stateData.members.isEmpty)

      val (fsm2, actorSystem2, testKit2) = {
        implicit val (actorSystem2, testKit2) = testActorSystem(2)
        (createRevaultActor(waitWhileActivates = false), actorSystem2, testKit2)
      }

      testKit2.awaitCond(fsm2.stateName == RevaultMemberStatus.Active && fsm2.stateData.members.nonEmpty, 5 second)

      shutdownRevaultActor(fsm1)(actorSystem1)
      shutdownCluster(1)
      shutdownActorSystem(1)

      testKit2.awaitCond(fsm2.stateName == RevaultMemberStatus.Active && fsm2.stateData.members.isEmpty, 10 second)
      shutdownRevaultActor(fsm2)(actorSystem2)
    }

    "Tasks should distribute to corresponding actors" in {
      val (fsm1, actorSystem1, testKit1, address1) = {
        implicit val (actorSystem1, testKit1) = testActorSystem(1)
        (createRevaultActor(waitWhileActivates = false), actorSystem1, testKit1, Cluster(actorSystem1).selfAddress.toString)
      }

      val (fsm2, actorSystem2, testKit2, address2) = {
        implicit val (actorSystem2, testKit2) = testActorSystem(2)
        (createRevaultActor(waitWhileActivates = false), actorSystem2, testKit2, Cluster(actorSystem2).selfAddress.toString)
      }

      testKit1.awaitCond(fsm1.stateName == RevaultMemberStatus.Active && fsm1.stateData.members.nonEmpty)
      testKit2.awaitCond(fsm2.stateName == RevaultMemberStatus.Active && fsm2.stateData.members.nonEmpty)

      val task1 = Task("abc", TestTaskContent("t1"))
      fsm1 ! task1
      testKit1.awaitCond(task1.isProcessed)
      task1.processorPath should include(address1)

      val task2 = Task("klm", TestTaskContent("t2"))
      fsm2 ! task2
      testKit2.awaitCond(task2.isProcessed)
      task2.processorPath should include(address2)
    }

    "Tasks should be forwarded to corresponding actors" in {
      val (fsm1, actorSystem1, testKit1, address1) = {
        implicit val (actorSystem1, testKit1) = testActorSystem(1)
        (createRevaultActor(waitWhileActivates = false), actorSystem1, testKit1, Cluster(actorSystem1).selfAddress.toString)
      }

      val (fsm2, actorSystem2, testKit2, address2) = {
        implicit val (actorSystem2, testKit2) = testActorSystem(2)
        (createRevaultActor(waitWhileActivates = false), actorSystem2, testKit2, Cluster(actorSystem2).selfAddress.toString)
      }

      testKit1.awaitCond(fsm1.stateName == RevaultMemberStatus.Active && fsm1.stateData.members.nonEmpty, 5 second)
      testKit2.awaitCond(fsm2.stateName == RevaultMemberStatus.Active && fsm2.stateData.members.nonEmpty, 5 second)

      val task1 = Task("abc", TestTaskContent("t3"))
      fsm2 ! task1
      testKit1.awaitCond(task1.isProcessed)
      task1.processorPath should include(address1)

      val task2 = Task("klm", TestTaskContent("t4"))
      fsm1 ! task2
      testKit2.awaitCond(task2.isProcessed)
      task2.processorPath should include(address2)
    }

    "Tasks for deactivating actor shouldn't be processed before deactivation complete" in {
      val (fsm1, actorSystem1, testKit1, address1) = {
        implicit val (actorSystem1, testKit1) = testActorSystem(1)
        (createRevaultActor(waitWhileActivates = false), actorSystem1, testKit1, Cluster(actorSystem1).selfAddress.toString)
      }

      val (fsm2, actorSystem2, testKit2, address2) = {
        implicit val (actorSystem2, testKit2) = testActorSystem(2)
        (createRevaultActor(waitWhileActivates = false), actorSystem2, testKit2, Cluster(actorSystem2).selfAddress.toString)
      }

      testKit1.awaitCond(fsm1.stateName == RevaultMemberStatus.Active && fsm1.stateData.members.nonEmpty)
      testKit2.awaitCond(fsm2.stateName == RevaultMemberStatus.Active && fsm2.stateData.members.nonEmpty)

      fsm1 ! ShutdownProcessor

      testKit1.awaitCond({
        fsm1.stateName == RevaultMemberStatus.Deactivating
      }, 10.second)

      val task1 = Task("abc", TestTaskContent("t5", sleep = 500))
      fsm1 ! task1

      val task2 = Task("abc", TestTaskContent("t6", sleep = 500))
      fsm2 ! task2

      val c1 = Cluster(actorSystem1)
      c1.down(c1.selfAddress)

      testKit2.awaitCond({
        assert(!(
          fsm2.stateData.members.filterNot(_._2.status == RevaultMemberStatus.Passive).nonEmpty
          &&
          (task2.isProcessed || task1.isProcessed)
          ))
        fsm2.stateData.members.isEmpty
      }, 10 second)

      testKit2.awaitCond(task1.isProcessed && task2.isProcessed)
      task1.processorPath should include(address2)
      task2.processorPath should include(address2)
    }

    "Processor should not confirm sync/activation until completes processing corresponding task" in {
      val (fsm1, actorSystem1, testKit1, address1) = {
        implicit val (actorSystem1, testKit1) = testActorSystem(1)
        (createRevaultActor(), actorSystem1, testKit1, Cluster(actorSystem1).selfAddress.toString)
      }

      val task1 = Task("klm", TestTaskContent("t7", sleep = 6000))
      fsm1 ! task1
      val task2 = Task("klm", TestTaskContent("t8"))
      fsm1 ! task2
      testKit1.awaitCond(task1.isProcessingStarted)

      val (fsm2, actorSystem2, testKit2, address2) = {
        implicit val (actorSystem2, testKit2) = testActorSystem(2)
        (createRevaultActor(waitWhileActivates = false), actorSystem2, testKit2, Cluster(actorSystem2).selfAddress.toString)
      }

      testKit1.awaitCond({
        assert(fsm2.stateName == RevaultMemberStatus.Activating)
        task1.isProcessed
      }, 10 second)

      task1.processorPath should include(address1)
      testKit2.awaitCond(task2.isProcessed, 10 second)
      task2.processorPath should include(address2)
    }
  }
}
