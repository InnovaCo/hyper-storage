import eu.inn.revault._
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

class TestSingleNode extends FreeSpec with ScalaFutures with TestHelpers {
  "TestProcessor in a single-node cluster" - {
    "ProcessorFSM should become Active" in {
      implicit val (as, testKit) = testActorSystem()
      createRevaultActor()
    }

    "ProcessorFSM should shutdown gracefully" in {
      implicit val (as, t) = testActorSystem()
      val fsm = createRevaultActor()
      shutdownRevaultActor(fsm)
    }

    "ProcessorFSM should process task" in {
      implicit val (as, testKit) = testActorSystem()
      val fsm = createRevaultActor()
      val task = Task("abc", TestTaskContent("t1"))
      fsm ! task
      testKit.awaitCond(TestRegistry.processed.exists(_._2 == task))
      shutdownRevaultActor(fsm)
    }

    "ProcessorFSM should stash task when workers are busy and process later" in {
      implicit val (as, testKit) = testActorSystem()
      val fsm = createRevaultActor()
      val task1 = Task("abc1", TestTaskContent("t1"))
      val task2 = Task("abc2", TestTaskContent("t2"))
      fsm ! task1
      fsm ! task2
      testKit.awaitCond(TestRegistry.processed.exists(_._2 == task1))
      testKit.awaitCond(TestRegistry.processed.exists(_._2 == task2))
      shutdownRevaultActor(fsm)
    }
  }
}
