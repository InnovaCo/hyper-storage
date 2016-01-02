import java.util.concurrent.atomic.AtomicInteger

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.event.LoggingReceive
import akka.testkit.{TestProbe, TestKit, TestFSMRef, TestActorRef}
import akka.text.GuardianExtractor
import com.typesafe.config.ConfigFactory
import eu.inn.hyperbus.transport._
import eu.inn.revault._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest._

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}

class TestSingleNode extends FreeSpec with ScalaFutures with TestHelpers {
  "TestProcessor in a single-node cluster" - {
    "ProcessorFSM should become Active" in {
      implicit val (as, testKit) = testActorSystem()
      createRevaultActor()
    }

    "ProcessorFSM should shutdown gracefully" in {
      implicit val (as, t) = testActorSystem()
      val fsm = createRevaultActor()

      val probe = TestProbe()
      probe watch fsm
      fsm ! ShutdownProcessor
      t.awaitCond {
        fsm.stateName == RevaultMemberStatus.Deactivating
      }
      probe.expectTerminated(fsm)
    }

    "ProcessorFSM should process task" in {
      implicit val (as, testKit) = testActorSystem()
      val fsm = createRevaultActor()
      val task = Task("abc", TestTaskContent("t1"))
      fsm ! task
      testKit.awaitCond {
        TestRegistry.processed.exists(_._2 == task)
      }
    }

    "ProcessorFSM should stash task when workers are busy and process later" in {
      implicit val (as, testKit) = testActorSystem()
      val fsm = createRevaultActor()
      val task1 = Task("abc1", TestTaskContent("t1"))
      val task2 = Task("abc2", TestTaskContent("t2"))
      fsm ! task1
      fsm ! task2
      testKit.awaitCond {
        TestRegistry.processed.exists(_._2 == task1)
      }
      testKit.awaitCond {
        TestRegistry.processed.exists(_._2 == task2)
      }
    }
  }
}
