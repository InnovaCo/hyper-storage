import eu.inn.revault._
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import scala.concurrent.duration._

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

    "ProcessorFSM should stash task when URL is 'locked' and it process later" in {
      implicit val (as, testKit) = testActorSystem()
      val fsm = createRevaultActor(2)
      val task1 = Task("abc1", TestTaskContent("t1", 500))
      val task1x = Task("abc1", TestTaskContent("t1x", 500))
      val task2 = Task("abc2", TestTaskContent("t2", 500))
      val task2x = Task("abc2", TestTaskContent("t2x", 500))
      fsm ! task1
      fsm ! task1x
      fsm ! task2
      fsm ! task2x
      testKit.awaitCond ({
        TestRegistry.processed.exists(_._2 == task1) && !TestRegistry.processed.exists(_._2 == task1x) &&
        TestRegistry.processed.exists(_._2 == task2) && !TestRegistry.processed.exists(_._2 == task2x)
      }, 750.milli)
      testKit.awaitCond({
        TestRegistry.processed.exists(_._2 == task1x) && TestRegistry.processed.exists(_._2 == task2x)
      }, 2.second)
      shutdownRevaultActor(fsm)
    }
  }
}
