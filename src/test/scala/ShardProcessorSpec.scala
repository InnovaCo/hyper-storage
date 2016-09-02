import eu.inn.hyperstorage.sharding._
import org.scalatest.concurrent.PatienceConfiguration.{Timeout ⇒ TestTimeout}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FreeSpec, Matchers}

import scala.concurrent.duration._

class ShardProcessorSpec extends FreeSpec
  with Matchers
  with ScalaFutures
  with TestHelpers
  with Eventually {

  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(10000, Millis)))

  "Shard processor in a single-node cluster" - {
    "ShardProcessor should become Active" in {
      implicit val as = testActorSystem()
      createShardProcessor("test-group")
    }

    "ShardProcessor should shutdown gracefully" in {
      implicit val as = testActorSystem()
      val fsm = createShardProcessor("test-group")
      shutdownShardProcessor(fsm)
    }

    "ShardProcessor should process task" in {
      implicit val as = testActorSystem()
      val fsm = createShardProcessor("test-group")
      val task = TestShardTask("abc", "t1")
      fsm ! task
      testKit().awaitCond(task.isProcessed)
      shutdownShardProcessor(fsm)
    }

    "ShardProcessor should stash task while Activating and process it later" in {
      implicit val as = testActorSystem()
      val fsm = createShardProcessor("test-group", waitWhileActivates = false)
      val task = TestShardTask("abc", "t1")
      fsm ! task
      fsm.stateName should equal(ShardMemberStatus.Activating)
      task.isProcessed should equal(false)
      testKit().awaitCond(task.isProcessed)
      fsm.stateName should equal(ShardMemberStatus.Active)
      shutdownShardProcessor(fsm)
    }

    "ShardProcessor should stash task when workers are busy and process later" in {
      implicit val as = testActorSystem()
      val tk = testKit()
      val fsm = createShardProcessor("test-group")
      val task1 = TestShardTask("abc1", "t1")
      val task2 = TestShardTask("abc2", "t2")
      fsm ! task1
      fsm ! task2
      tk.awaitCond(task1.isProcessed)
      tk.awaitCond(task2.isProcessed)
      shutdownShardProcessor(fsm)
    }

    "ShardProcessor should stash task when URL is 'locked' and it process later" in {
      implicit val as = testActorSystem()
      val tk = testKit()
      val fsm = createShardProcessor("test-group", 2)
      val task1 = TestShardTask("abc1", "t1", 500)
      val task1x = TestShardTask("abc1", "t1x", 500)
      val task2 = TestShardTask("abc2", "t2", 500)
      val task2x = TestShardTask("abc2", "t2x", 500)
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
      shutdownShardProcessor(fsm)
    }
  }
}
