import java.io.ByteArrayInputStream
import java.util.UUID

import akka.testkit.{ImplicitSender, TestActorRef, TestProbe}
import eu.inn.binders.dynamic.{Null, Obj, Text}
import eu.inn.hyperbus.model.{Body, Response, DynamicBody}
import eu.inn.hyperbus.model.standard._
import eu.inn.hyperbus.serialization.MessageDeserializer
import eu.inn.hyperbus.transport.ActorSystemRegistry
import eu.inn.hyperbus.util.StringSerializer
import eu.inn.revault.protocol.{RevaultDelete, RevaultPatch, Monitor, RevaultPut}
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
        DynamicBody(Obj(Map("text" → Text("Test resource value"), "null" → Null)))
      )

      whenReady(db.selectContent("/test-resource-1", "")) { result =>
        result shouldBe None
      }

      val taskStr = StringSerializer.serializeToString(task)
      worker ! RevaultTask("", System.currentTimeMillis() + 10000, probeClient.ref, taskStr)
      expectMsg(ReadyForNextTask)
      probeClient.expectMsgPF() {
        case result: RevaultTaskResult if response(result.content).status == Status.OK &&
          response(result.content).correlationId == task.correlationId ⇒ {
          true
        }
      }

      whenReady(db.selectContent("/test-resource-1", "")) { result =>
        result.get.body should equal(Some("""{"text":"Test resource value"}"""))
      }
    }

    "Patch resource that doesn't exists" in {
      val hyperBus = testHyperBus()
      val tk = testKit()
      import tk._

      val worker = TestActorRef(new WorkerActor(hyperBus, db))
      val probeClient = new TestProbe(system)

      val task = RevaultPatch(
        path = "/not-existing",
        DynamicBody(Obj(Map("text" → Text("Test resource value"))))
      )

      val taskStr = StringSerializer.serializeToString(task)
      worker ! RevaultTask("", System.currentTimeMillis() + 10000, probeClient.ref, taskStr)
      expectMsg(ReadyForNextTask)
      probeClient.expectMsgPF() {
        case result: RevaultTaskResult if response(result.content).status == Status.NOT_FOUND &&
          response(result.content).correlationId == task.correlationId ⇒ {
          true
        }
      }

      whenReady(db.selectContent("/not-existing", "")) { result =>
        result shouldBe None
      }
    }

    "Patch existing resource" in {
      val hyperBus = testHyperBus()
      val tk = testKit()
      import tk._

      val worker = TestActorRef(new WorkerActor(hyperBus, db))
      val probeClient = new TestProbe(system)

      val path = "/test-resource-" + UUID.randomUUID().toString
      val taskPutStr = StringSerializer.serializeToString(RevaultPut(path,
        DynamicBody(Obj(Map("text1" → Text("abc"), "text2" → Text("klmn"))))
      ))

      worker ! RevaultTask("", System.currentTimeMillis() + 10000, probeClient.ref, taskPutStr)
      expectMsg(ReadyForNextTask)
      probeClient.expectMsgType[RevaultTaskResult]

      val task = RevaultPatch(path,
        DynamicBody(Obj(Map("text1" → Text("efg"), "text2" → Null, "text3" → Text("zzz"))))
      )
      val taskPatchStr = StringSerializer.serializeToString(task)

      worker ! RevaultTask("", System.currentTimeMillis() + 10000, probeClient.ref, taskPatchStr)
      expectMsg(ReadyForNextTask)

      probeClient.expectMsgPF() {
        case result: RevaultTaskResult if response(result.content).status == Status.OK &&
          response(result.content).correlationId == task.correlationId ⇒ {
          true
        }
      }

      whenReady(db.selectContent(path, "")) { result =>
        result.get.body should equal(Some("""{"text1":"efg","text3":"zzz"}"""))
      }
    }

    "Delete resource that doesn't exists" in {
      val hyperBus = testHyperBus()
      val tk = testKit()
      import tk._

      val worker = TestActorRef(new WorkerActor(hyperBus, db))
      val probeClient = new TestProbe(system)

      val task = RevaultDelete(path = "/not-existing", body = EmptyBody)

      val taskStr = StringSerializer.serializeToString(task)
      worker ! RevaultTask("", System.currentTimeMillis() + 10000, probeClient.ref, taskStr)
      expectMsg(ReadyForNextTask)
      probeClient.expectMsgPF() {
        case result: RevaultTaskResult if response(result.content).status == Status.NOT_FOUND &&
          response(result.content).correlationId == task.correlationId ⇒ {
          true
        }
      }

      whenReady(db.selectContent("/not-existing", "")) { result =>
        result shouldBe None
      }
    }

    "Delete resource that exists" in {
      val hyperBus = testHyperBus()
      val tk = testKit()
      import tk._

      val worker = TestActorRef(new WorkerActor(hyperBus, db))
      val probeClient = new TestProbe(system)

      val path = "/test-resource-" + UUID.randomUUID().toString
      val taskPutStr = StringSerializer.serializeToString(RevaultPut(path,
        DynamicBody(Obj(Map("text1" → Text("abc"), "text2" → Text("klmn"))))
      ))

      worker ! RevaultTask("", System.currentTimeMillis() + 10000, probeClient.ref, taskPutStr)
      expectMsg(ReadyForNextTask)
      probeClient.expectMsgType[RevaultTaskResult]

      whenReady(db.selectContent(path, "")) { result =>
        result shouldNot be(None)
      }

      val task = RevaultDelete(path, body = EmptyBody)

      val taskStr = StringSerializer.serializeToString(task)
      worker ! RevaultTask("", System.currentTimeMillis() + 10000, probeClient.ref, taskStr)
      expectMsg(ReadyForNextTask)
      probeClient.expectMsgPF() {
        case result: RevaultTaskResult if response(result.content).status == Status.OK &&
          response(result.content).correlationId == task.correlationId ⇒ {
          true
        }
      }

      whenReady(db.selectContent(path, "")) { result =>
        result.get.isDeleted shouldBe true
      }
    }
  }

  def response(content: String): Response[Body] = {
    val byteStream = new ByteArrayInputStream(content.getBytes("UTF-8"))
    MessageDeserializer.deserializeResponseWith(byteStream) { (responseHeader, responseBodyJson) =>
      val body: Body = if (responseHeader.status >= 400) {
        ErrorBody(responseHeader.contentType, responseBodyJson)
      }
      else {
        DynamicBody(responseHeader.contentType, responseBodyJson)
      }
      StandardResponse(responseHeader, body)
    }
  }
}
