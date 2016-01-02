import eu.inn.revault.RevaultMemberStatus
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

class TestTwoNode extends FreeSpec with ScalaFutures with TestHelpers {
  "TestProcessor in a two-node cluster" - {
    "ProcessorFSM should become Active" in {
      val (fsm1, actorSystem1, testKit1) = {
        implicit val (actorSystem1, testKit1) = testActorSystem(1)
        (createRevaultActor(), actorSystem1, testKit1)
      }

      val (fsm2, actorSystem2, testKit2) = {
        implicit val (actorSystem2, testKit2) = testActorSystem(2)
        (createRevaultActor(), actorSystem2, testKit2)
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
        (createRevaultActor(), actorSystem1, testKit1)
      }

      testKit1.awaitCond(fsm1.stateName == RevaultMemberStatus.Active && fsm1.stateData.members.isEmpty)

      val (fsm2, actorSystem2, testKit2) = {
        implicit val (actorSystem2, testKit2) = testActorSystem(2)
        (createRevaultActor(), actorSystem2, testKit2)
      }

      testKit2.awaitCond(fsm2.stateName == RevaultMemberStatus.Active && fsm2.stateData.members.nonEmpty, 5 second)

      shutdownRevaultActor(fsm1)(actorSystem1)
      shutdownCluster(1)
      shutdownActorSystem(1)

      testKit2.awaitCond(fsm2.stateName == RevaultMemberStatus.Active && fsm2.stateData.members.isEmpty, 10 second)
      shutdownRevaultActor(fsm2)(actorSystem2)
    }
  }
}
