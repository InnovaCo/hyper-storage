import java.io.ByteArrayInputStream

import akka.testkit.{ImplicitSender, TestActorRef, TestProbe}
import eu.inn.binders.dynamic.Text
import eu.inn.hyperbus.model.{Response, DynamicBody}
import eu.inn.hyperbus.model.standard.{Status, StandardResponse, Method}
import eu.inn.hyperbus.serialization.MessageDeserializer
import eu.inn.hyperbus.transport.ActorSystemRegistry
import eu.inn.revault.protocol.{Monitor, RevaultPut}
import eu.inn.revault.{ReadyForNextTask, RevaultTaskResult, RevaultTask, WorkerActor}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FreeSpec, Matchers}
import scala.concurrent.duration._

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

      whenReady(db.selectContent("/test-resource-1","")) { result =>
        result shouldBe None
      }

      worker ! RevaultTask("", System.currentTimeMillis()+10000, probeClient.ref, task.serializeToString())
      expectMsg(ReadyForNextTask)
      probeClient.expectMsgPF() {
        case result: RevaultTaskResult if response(result.content).status == Status.OK &&
          response(result.content).correlationId == task.correlationId ⇒ {
          whenReady(db.selectContent("/test-resource-1","")) { result =>
            result.get.body should equal(Some("Test resource value"))
          }
          true
        }
      }
    }
  }

  def response(content: String): Response[Monitor] = {
    val byteStream = new ByteArrayInputStream(content.getBytes("UTF-8"))
    MessageDeserializer.deserializeResponseWith(byteStream) { (responseHeader, responseBodyJson) =>
      val body = Monitor(responseHeader.contentType, responseBodyJson)
      StandardResponse(responseHeader, body).asInstanceOf[Response[Monitor]]
    }
  }
}
