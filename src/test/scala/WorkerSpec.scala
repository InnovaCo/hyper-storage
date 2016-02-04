import akka.testkit.{ImplicitSender, TestActorRef, TestProbe}
import eu.inn.binders.dynamic.Text
import eu.inn.hyperbus.model.DynamicBody
import eu.inn.hyperbus.transport.ActorSystemRegistry
import eu.inn.revault.protocol.RevaultPut
import eu.inn.revault.{ReadyForNextTask, RevaultTaskResult, RevaultTask, WorkerActor}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FreeSpec, Matchers}

class WorkerSpec extends FreeSpec
  with Matchers
  with ScalaFutures
  with CassandraFixture
  with TestHelpers {

  "Worker" - {
    "Put Task" in {
      val hyperBus = testHyperBus()
      val tk = testKit()
      import tk._

      val worker = TestActorRef(new WorkerActor(hyperBus, db))
      val probeClient = new TestProbe(system)

      val task = RevaultPut(
        path = "/test-resource-1",
        DynamicBody(Text("Test resource value"))
      )

      worker ! RevaultTask("", System.currentTimeMillis()+10000, probeClient.ref, task.serializeToString())
      expectMsg(ReadyForNextTask)
      probeClient expectMsg RevaultTaskResult("ha ha")
    }
  }
}
