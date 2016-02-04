import eu.inn.revault.{RevaultMemberStatus, Task}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import scala.concurrent.duration._

class SingleNodeSpec extends FreeSpec with ScalaFutures with TestHelpers {
  "Processor in a single-node cluster" - {
    "ProcessorFSM should become Active" in {
      implicit val as = testActorSystem()
      createRevaultActor()
    }

    "ProcessorFSM should shutdown gracefully" in {
      implicit val as = testActorSystem()
      val fsm = createRevaultActor()
      shutdownRevaultActor(fsm)
    }

    "ProcessorFSM should process task" in {
      implicit val as = testActorSystem()
      val fsm = createRevaultActor()
      val task = TestTask("abc", "t1")
      fsm ! task
      testKit().awaitCond(task.isProcessed)
      shutdownRevaultActor(fsm)
    }

    "ProcessorFSM should stash task while Activating and process it later" in {
      implicit val as = testActorSystem()
      val fsm = createRevaultActor(waitWhileActivates = false)
      val task = TestTask("abc", "t1")
      fsm ! task
      fsm.stateName should equal(RevaultMemberStatus.Activating)
      task.isProcessed should equal(false)
      testKit().awaitCond(task.isProcessed)
      fsm.stateName should equal(RevaultMemberStatus.Active)
      shutdownRevaultActor(fsm)
    }

    "ProcessorFSM should stash task when workers are busy and process later" in {
      implicit val as = testActorSystem()
      val tk = testKit()
      val fsm = createRevaultActor()
      val task1 = TestTask("abc1", "t1")
      val task2 = TestTask("abc2", "t2")
      fsm ! task1
      fsm ! task2
      tk.awaitCond(task1.isProcessed)
      tk.awaitCond(task2.isProcessed)
      shutdownRevaultActor(fsm)
    }

    "ProcessorFSM should stash task when URL is 'locked' and it process later" in {
      implicit val as = testActorSystem()
      val tk = testKit()
      val fsm = createRevaultActor(2)
      val task1 = TestTask("abc1", "t1", 500)
      val task1x = TestTask("abc1", "t1x", 500)
      val task2 = TestTask("abc2", "t2", 500)
      val task2x = TestTask("abc2", "t2x", 500)
      fsm ! task1
      fsm ! task1x
      fsm ! task2
      fsm ! task2x
      tk.awaitCond({
        task1.isProcessed && !task1x.isProcessed &&
          task2.isProcessed && !task2x.isProcessed
      }, 750.milli)
      tk.awaitCond({
        task1x.isProcessed && task2x.isProcessed
      }, 2.second)
      shutdownRevaultActor(fsm)
    }
  }
}
