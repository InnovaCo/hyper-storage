import java.util.UUID

import akka.actor.Props
import akka.testkit.{TestActorRef, TestProbe}
import akka.util.Timeout
import com.datastax.driver.core.utils.UUIDs
import eu.inn.binders.dynamic.{Null, Obj, Text}
import eu.inn.hyperbus.serialization.{StringDeserializer,StringSerializer}
import eu.inn.hyperbus.model._
import eu.inn.revault._
import eu.inn.revault.protocol._
import eu.inn.revault.sharding.{ShardProcessor, ShardMemberStatus, ShardTaskComplete}
import mock.FaultClientTransport
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FreeSpec, Matchers}

import scala.concurrent.{Promise, Future}
import scala.concurrent.duration._
import scala.util.control.NonFatal
import org.scalatest.concurrent.PatienceConfiguration.{Timeout ⇒ TestTimeout}

// todo: split revault, shardprocessor and single nodes test
class RevaultSpec extends FreeSpec
  with Matchers
  with ScalaFutures
  with CassandraFixture
  with TestHelpers {

  import ContentLogic._

  "Revault" - {
    "Processor in a single-node cluster" - {
      "ShardProcessor should become Active" in {
        implicit val as = testActorSystem()
        createRevaultActor("test-group")
      }

      "ShardProcessor should shutdown gracefully" in {
        implicit val as = testActorSystem()
        val fsm = createRevaultActor("test-group")
        shutdownRevaultActor(fsm)
      }

      "ShardProcessor should process task" in {
        implicit val as = testActorSystem()
        val fsm = createRevaultActor("test-group")
        val task = TestShardTask("abc", "t1")
        fsm ! task
        testKit().awaitCond(task.isProcessed)
        shutdownRevaultActor(fsm)
      }

      "ShardProcessor should stash task while Activating and process it later" in {
        implicit val as = testActorSystem()
        val fsm = createRevaultActor("test-group", waitWhileActivates = false)
        val task = TestShardTask("abc", "t1")
        fsm ! task
        fsm.stateName should equal(ShardMemberStatus.Activating)
        task.isProcessed should equal(false)
        testKit().awaitCond(task.isProcessed)
        fsm.stateName should equal(ShardMemberStatus.Active)
        shutdownRevaultActor(fsm)
      }

      "ShardProcessor should stash task when workers are busy and process later" in {
        implicit val as = testActorSystem()
        val tk = testKit()
        val fsm = createRevaultActor("test-group")
        val task1 = TestShardTask("abc1", "t1")
        val task2 = TestShardTask("abc2", "t2")
        fsm ! task1
        fsm ! task2
        tk.awaitCond(task1.isProcessed)
        tk.awaitCond(task2.isProcessed)
        shutdownRevaultActor(fsm)
      }

      "ShardProcessor should stash task when URL is 'locked' and it process later" in {
        implicit val as = testActorSystem()
        val tk = testKit()
        val fsm = createRevaultActor("test-group", 2)
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
        shutdownRevaultActor(fsm)
      }
    }

    "Put Task" in {
      val hyperBus = testHyperBus()
      val tk = testKit()
      import tk._

      val worker = TestActorRef(new RevaultWorker(hyperBus, db, 10000))

      val task = RevaultPut(
        path = "/test-resource-1",
        DynamicBody(Obj(Map("text" → Text("Test resource value"), "null" → Null)))
      )

      whenReady(db.selectContent("/test-resource-1", "")) { result =>
        result shouldBe None
      }

      val taskStr = StringSerializer.serializeToString(task)
      worker ! RevaultTask("", System.currentTimeMillis() + 10000, taskStr)
      val completerTask = expectMsgType[RevaultCompleterTask]
      completerTask.path should equal(task.path)
      val workerResult = expectMsgType[ShardTaskComplete]
      val r = response(workerResult.result.asInstanceOf[RevaultTaskResult].content)
      r.status should equal(Status.ACCEPTED)
      r.correlationId should equal(task.correlationId)

      val uuid = whenReady(db.selectContent("/test-resource-1", "")) { result =>
        result.get.body should equal(Some("""{"text":"Test resource value"}"""))
        result.get.monitorList.size should equal(1)

        selectMonitors(result.get.monitorList, result.get.uri, db) foreach { monitor ⇒
          monitor.completedAt shouldBe None
          monitor.revision should equal(result.get.revision)
        }
        result.get.monitorList.head
      }

      val completer = TestActorRef(new RevaultCompleter(hyperBus, db))
      completer ! completerTask
      val completerResult = expectMsgType[ShardTaskComplete]
      val rc = completerResult.result.asInstanceOf[RevaultCompleterTaskResult]
      rc.path should equal("/test-resource-1")
      rc.monitors should contain(uuid)
      selectMonitors(rc.monitors, "/test-resource-1", db) foreach { monitor ⇒
        monitor.completedAt shouldNot be(None)
        monitor.revision should equal(1)
      }
    }

    "Patch resource that doesn't exists" in {
      val hyperBus = testHyperBus()
      val tk = testKit()
      import tk._

      val worker = TestActorRef(new RevaultWorker(hyperBus, db, 10000))

      val task = RevaultPatch(
        path = "/not-existing",
        DynamicBody(Obj(Map("text" → Text("Test resource value"))))
      )

      val taskStr = StringSerializer.serializeToString(task)
      worker ! RevaultTask("", System.currentTimeMillis() + 10000, taskStr)
      expectMsgPF() {
        case ShardTaskComplete(_, result : RevaultTaskResult) if response(result.content).status == Status.NOT_FOUND &&
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

      val worker = TestActorRef(new RevaultWorker(hyperBus, db, 10000))

      val path = "/test-resource-" + UUID.randomUUID().toString
      val taskPutStr = StringSerializer.serializeToString(RevaultPut(path,
        DynamicBody(Obj(Map("text1" → Text("abc"), "text2" → Text("klmn"))))
      ))

      worker ! RevaultTask("", System.currentTimeMillis() + 10000, taskPutStr)
      expectMsgType[RevaultCompleterTask]
      expectMsgType[ShardTaskComplete]

      val task = RevaultPatch(path,
        DynamicBody(Obj(Map("text1" → Text("efg"), "text2" → Null, "text3" → Text("zzz"))))
      )
      val taskPatchStr = StringSerializer.serializeToString(task)

      worker ! RevaultTask("", System.currentTimeMillis() + 10000, taskPatchStr)
      expectMsgType[RevaultCompleterTask]
      expectMsgPF() {
        case ShardTaskComplete(_, result : RevaultTaskResult) if response(result.content).status == Status.ACCEPTED &&
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

      val worker = TestActorRef(new RevaultWorker(hyperBus, db, 10000))

      val task = RevaultDelete(path = "/not-existing", body = EmptyBody)

      val taskStr = StringSerializer.serializeToString(task)
      worker ! RevaultTask("", System.currentTimeMillis() + 10000, taskStr)
      expectMsgPF() {
        case ShardTaskComplete(_, result : RevaultTaskResult) if response(result.content).status == Status.NOT_FOUND &&
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

      val worker = TestActorRef(new RevaultWorker(hyperBus, db, 10000))

      val path = "/test-resource-" + UUID.randomUUID().toString
      val taskPutStr = StringSerializer.serializeToString(RevaultPut(path,
        DynamicBody(Obj(Map("text1" → Text("abc"), "text2" → Text("klmn"))))
      ))

      worker ! RevaultTask("", System.currentTimeMillis() + 10000, taskPutStr)
      expectMsgType[RevaultCompleterTask]
      expectMsgType[ShardTaskComplete]

      whenReady(db.selectContent(path, "")) { result =>
        result shouldNot be(None)
      }

      val task = RevaultDelete(path, body = EmptyBody)

      val taskStr = StringSerializer.serializeToString(task)
      worker ! RevaultTask("", System.currentTimeMillis() + 10000, taskStr)
      expectMsgType[RevaultCompleterTask]
      expectMsgPF() {
        case ShardTaskComplete(_, result : RevaultTaskResult) if response(result.content).status == Status.ACCEPTED &&
          response(result.content).correlationId == task.correlationId ⇒ {
          true
        }
      }

      whenReady(db.selectContent(path, "")) { result =>
        result.get.isDeleted shouldBe true
      }
    }
  }

  "Test multiple monitors" in {
    val hyperBus = testHyperBus()
    val tk = testKit()
    import tk._

    val worker = TestActorRef(new RevaultWorker(hyperBus, db, 10000))
    val path = "/abcde"
    val taskStr1 = StringSerializer.serializeToString(RevaultPut(path,
      DynamicBody(Obj(Map("text" → Text("Test resource value"), "null" → Null)))
    ))
    worker ! RevaultTask("", System.currentTimeMillis() + 10000, taskStr1)
    expectMsgType[RevaultCompleterTask]
    expectMsgType[ShardTaskComplete]

    val taskStr2 = StringSerializer.serializeToString(RevaultPatch(path,
      DynamicBody(Obj(Map("text" → Text("abc"), "text2" → Text("klmn"))))
    ))
    worker ! RevaultTask("", System.currentTimeMillis() + 10000, taskStr2)
    expectMsgType[RevaultCompleterTask]
    expectMsgType[ShardTaskComplete]

    val taskStr3 = StringSerializer.serializeToString(RevaultDelete(path,EmptyBody))
    worker ! RevaultTask("", System.currentTimeMillis() + 10000, taskStr3)
    val completerTask = expectMsgType[RevaultCompleterTask]
    val workerResult = expectMsgType[ShardTaskComplete]
    val r = response(workerResult.result.asInstanceOf[RevaultTaskResult].content)
    r.status should equal(Status.ACCEPTED)

    val monitors = whenReady(db.selectContent(path, "")) { result =>
      result.get.isDeleted should equal(true)
      result.get.monitorList
    }

    selectMonitors(monitors, path, db) foreach { monitor ⇒
      monitor.completedAt should be(None)
    }

    val completer = TestActorRef(new RevaultCompleter(hyperBus, db))
    completer ! completerTask
    val completerResult = expectMsgType[ShardTaskComplete]
    val rc = completerResult.result.asInstanceOf[RevaultCompleterTaskResult]
    rc.path should equal(path)
    rc.monitors should equal(monitors.reverse)

    selectMonitors(rc.monitors, path, db) foreach { monitor ⇒
      monitor.completedAt shouldNot be(None)
    }
  }

  "Test faulty publish" in {
    val hyperBus = testHyperBus()
    val tk = testKit()
    import tk._

    val worker = TestActorRef(new RevaultWorker(hyperBus, db, 10000))
    val path = "/faulty"
    val taskStr1 = StringSerializer.serializeToString(RevaultPut(path,
      DynamicBody(Obj(Map("text" → Text("Test resource value"), "null" → Null)))
    ))
    worker ! RevaultTask("", System.currentTimeMillis() + 10000, taskStr1)
    expectMsgType[RevaultCompleterTask]
    expectMsgType[ShardTaskComplete]

    val taskStr2 = StringSerializer.serializeToString(RevaultPatch(path,
      DynamicBody(Obj(Map("text" → Text("abc"), "text2" → Text("klmn"))))
    ))
    worker ! RevaultTask("", System.currentTimeMillis() + 10000, taskStr2)
    val completerTask = expectMsgType[RevaultCompleterTask]
    expectMsgType[ShardTaskComplete]

    val monitorUuids = whenReady(db.selectContent(path, "")) { result =>
      result.get.monitorList
    }

    selectMonitors(monitorUuids, path, db) foreach { _.completedAt shouldBe None }

    val completer = TestActorRef(new RevaultCompleter(hyperBus, db))

    FaultClientTransport.checkers += {
      case request: DynamicRequest ⇒
        if (request.method == Method.FEED_PUT) {
          true
        } else {
          fail("Unexpected publish")
          false
        }
    }

    completer ! completerTask
    expectMsgType[ShardTaskComplete].result shouldBe a[CompletionFailedException]
    selectMonitors(monitorUuids, path, db) foreach { _.completedAt shouldBe None }

    FaultClientTransport.checkers.clear()
    FaultClientTransport.checkers += {
      case request: DynamicRequest ⇒
        if (request.method == Method.FEED_PATCH) {
          true
        } else {
          false
        }
    }

    completer ! completerTask
    expectMsgType[ShardTaskComplete].result shouldBe a[CompletionFailedException]
    val mons = selectMonitors(monitorUuids, path, db)
    println(mons)
    mons.head.completedAt shouldBe None
    mons.tail.head.completedAt shouldNot be(None)

    FaultClientTransport.checkers.clear()
    completer ! completerTask
    expectMsgType[ShardTaskComplete].result shouldBe a[RevaultCompleterTaskResult]
    selectMonitors(monitorUuids, path, db) foreach { _.completedAt shouldNot be(None) }
  }

  "Test revault (integrated)" in {
    val hyperBus = testHyperBus()
    val tk = testKit()
    import tk._
    import system._

    val workerProps = Props(classOf[RevaultWorker], hyperBus, db, 10000l)
    val completerProps = Props(classOf[RevaultCompleter], hyperBus, db)
    val workerSettings = Map(
      "revault" → (workerProps, 1),
      "revault-completer" → (completerProps, 1)
    )

    val processor = TestActorRef(new ShardProcessor(workerSettings, "revault"))
    val distributor = TestActorRef(new HyperbusAdapter(processor, db, 20 seconds))
    import eu.inn.hyperbus.akkaservice._
    implicit val timeout = Timeout(20.seconds)
    hyperBus.routeTo[HyperbusAdapter](distributor)

    val putEventPromise = Promise[RevaultFeedPut]()
    hyperBus |> { put: RevaultFeedPut ⇒
      Future {
        putEventPromise.success(put)
      }
    }

    Thread.sleep(2000)

    val path = "/" + UUID.randomUUID().toString
    whenReady(hyperBus <~ RevaultPut(path, DynamicBody(Text("Hello"))), TestTimeout(10.seconds)) { response ⇒
      response.status should equal (Status.ACCEPTED)
    }

    val putEventFuture = putEventPromise.future
    whenReady(putEventFuture) { putEvent ⇒
      putEvent.method should equal(Method.FEED_PUT)
      putEvent.body should equal (DynamicBody(Text("Hello")))
      putEvent.headers.get(Header.REVISION) shouldNot be(None)
    }

    whenReady(hyperBus <~ RevaultGet(path, EmptyBody), TestTimeout(10.seconds)) { response ⇒
      response.status should equal (Status.OK)
      response.body.content should equal(Text("Hello"))
    }
  }

  "Null patch with revault (integrated)" in {
    val hyperBus = testHyperBus()
    val tk = testKit()
    import tk._
    import system._

    val workerProps = Props(classOf[RevaultWorker], hyperBus, db, 10000l)
    val completerProps = Props(classOf[RevaultCompleter], hyperBus, db)
    val workerSettings = Map(
      "revault" → (workerProps, 1),
      "revault-completer" → (completerProps, 1)
    )

    val processor = TestActorRef(new ShardProcessor(workerSettings, "revault"))
    val distributor = TestActorRef(new HyperbusAdapter(processor, db, 20 seconds))
    import eu.inn.hyperbus.akkaservice._
    implicit val timeout = Timeout(20.seconds)
    hyperBus.routeTo[HyperbusAdapter](distributor)

    val patchEventPromise = Promise[RevaultFeedPatch]()
    hyperBus |> { patch: RevaultFeedPatch ⇒
      Future {
        patchEventPromise.success(patch)
      }
    }

    Thread.sleep(2000)

    val path = "/" + UUID.randomUUID().toString
    whenReady(hyperBus <~ RevaultPut(path, DynamicBody(
      Obj(Map("a" → Text("1"), "b" → Text("2"), "c" → Text("3")))
    )), TestTimeout(10.seconds)) { response ⇒
      response.status should equal (Status.ACCEPTED)
    }

    whenReady(hyperBus <~ RevaultPatch(path, DynamicBody(Obj(Map("b" → Null)))), TestTimeout(10.seconds)) { response ⇒
      response.status should equal (Status.ACCEPTED)
    }

    val patchEventFuture = patchEventPromise.future
    whenReady(patchEventFuture) { patchEvent ⇒
      patchEvent.method should equal(Method.FEED_PATCH)
      patchEvent.body should equal (DynamicBody(Obj(Map("b" → Null))))
      patchEvent.headers.get(Header.REVISION) shouldNot be(None)
    }

    whenReady(hyperBus <~ RevaultGet(path, EmptyBody), TestTimeout(10.seconds)) { response ⇒
      response.status should equal (Status.OK)
      response.body.content should equal(Obj(Map("a" → Text("1"), "c" → Text("3"))))
    }
  }

  def response(content: String): Response[Body] = StringDeserializer.dynamicResponse(content)
}
