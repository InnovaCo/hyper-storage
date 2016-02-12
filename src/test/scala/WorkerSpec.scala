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
import eu.inn.revault._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FreeSpec, Matchers}
import scala.concurrent.duration._

class WorkerSpec extends FreeSpec
  with Matchers
  with ScalaFutures
  with CassandraFixture
  with TestHelpers {

  "Revault" - {
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

    "Put Task" in {
      val hyperBus = testHyperBus()
      val tk = testKit()
      import tk._

      val worker = TestActorRef(new WorkerActor(hyperBus, db, system.deadLetters))

      val task = RevaultPut(
        path = "/test-resource-1",
        DynamicBody(Obj(Map("text" → Text("Test resource value"), "null" → Null)))
      )

      whenReady(db.selectContent("/test-resource-1", "")) { result =>
        result shouldBe None
      }

      val taskStr = StringSerializer.serializeToString(task)
      worker ! RevaultTask("", System.currentTimeMillis() + 10000, taskStr)
      expectMsgPF() {
        case WorkerTaskComplete(Some(result : RevaultTaskResult)) if response(result.content).status == Status.OK &&
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

      val worker = TestActorRef(new WorkerActor(hyperBus, db, system.deadLetters))

      val task = RevaultPatch(
        path = "/not-existing",
        DynamicBody(Obj(Map("text" → Text("Test resource value"))))
      )

      val taskStr = StringSerializer.serializeToString(task)
      worker ! RevaultTask("", System.currentTimeMillis() + 10000, taskStr)
      expectMsgPF() {
        case WorkerTaskComplete(Some(result: RevaultTaskResult)) if response(result.content).status == Status.NOT_FOUND &&
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

      val worker = TestActorRef(new WorkerActor(hyperBus, db, system.deadLetters))

      val path = "/test-resource-" + UUID.randomUUID().toString
      val taskPutStr = StringSerializer.serializeToString(RevaultPut(path,
        DynamicBody(Obj(Map("text1" → Text("abc"), "text2" → Text("klmn"))))
      ))

      worker ! RevaultTask("", System.currentTimeMillis() + 10000, taskPutStr)
      expectMsgType[WorkerTaskComplete]

      val task = RevaultPatch(path,
        DynamicBody(Obj(Map("text1" → Text("efg"), "text2" → Null, "text3" → Text("zzz"))))
      )
      val taskPatchStr = StringSerializer.serializeToString(task)

      worker ! RevaultTask("", System.currentTimeMillis() + 10000, taskPatchStr)
      expectMsgPF() {
        case WorkerTaskComplete(Some(result: RevaultTaskResult)) if response(result.content).status == Status.OK &&
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

      val worker = TestActorRef(new WorkerActor(hyperBus, db, system.deadLetters))

      val task = RevaultDelete(path = "/not-existing", body = EmptyBody)

      val taskStr = StringSerializer.serializeToString(task)
      worker ! RevaultTask("", System.currentTimeMillis() + 10000, taskStr)
      expectMsgPF() {
        case WorkerTaskComplete(Some(result: RevaultTaskResult)) if response(result.content).status == Status.NOT_FOUND &&
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

      val worker = TestActorRef(new WorkerActor(hyperBus, db, system.deadLetters))

      val path = "/test-resource-" + UUID.randomUUID().toString
      val taskPutStr = StringSerializer.serializeToString(RevaultPut(path,
        DynamicBody(Obj(Map("text1" → Text("abc"), "text2" → Text("klmn"))))
      ))

      worker ! RevaultTask("", System.currentTimeMillis() + 10000, taskPutStr)
      expectMsgType[WorkerTaskComplete]

      whenReady(db.selectContent(path, "")) { result =>
        result shouldNot be(None)
      }

      val task = RevaultDelete(path, body = EmptyBody)

      val taskStr = StringSerializer.serializeToString(task)
      worker ! RevaultTask("", System.currentTimeMillis() + 10000, taskStr)
      expectMsgPF() {
        case WorkerTaskComplete(Some(result: RevaultTaskResult)) if response(result.content).status == Status.OK &&
          response(result.content).correlationId == task.correlationId ⇒ {
          true
        }
      }

      whenReady(db.selectContent(path, "")) { result =>
        result.get.isDeleted shouldBe true
      }
    }
  }

  "Put Task failed" in {
    val hyperBus = testHyperBus()
    val tk = testKit()
    import tk._

    val probeRecoveryActor = new TestProbe(system)
    val worker = TestActorRef(new WorkerActor(hyperBus, db, probeRecoveryActor.ref))

    val task = RevaultPut(
      path = "/faulty",
      DynamicBody(Obj(Map("text" → Text("Test resource value"), "null" → Null)))
    )

    whenReady(db.selectContent("/faulty", "")) { result =>
      result shouldBe None
    }

    val taskStr = StringSerializer.serializeToString(task)
    worker ! RevaultTask("", System.currentTimeMillis() + 10000, taskStr)
    expectMsgPF() {
      case WorkerTaskComplete(Some(result: RevaultTaskResult)) if response(result.content).status == Status.ACCEPTED &&
        response(result.content).correlationId == task.correlationId ⇒ {
        true
      }
    }
    probeRecoveryActor.expectMsgType[RevaultTaskAccepted]

    whenReady(db.selectContent("/faulty", "")) { result =>
      result.get.body should equal(Some("""{"text":"Test resource value"}"""))

      val monitorChannel = result.get.monitorChannel
      val monitorDr = result.get.monitorDt

      whenReady(db.selectMonitor(monitorDr, monitorChannel, "/faulty")) { result =>
        result.get.completedAt shouldBe None
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
