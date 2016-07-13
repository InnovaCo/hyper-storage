import java.util.UUID

import akka.actor.{ActorSelection, Address}
import akka.pattern.gracefulStop
import akka.testkit.{TestActorRef, TestProbe}
import akka.util.Timeout
import com.datastax.driver.core.utils.UUIDs
import eu.inn.binders.value._
import eu.inn.hyperbus.model._
import eu.inn.hyperbus.model.utils.{Sort, SortBy}
import eu.inn.hyperbus.serialization.{StringDeserializer, StringSerializer}
import eu.inn.hyperstorage._
import eu.inn.hyperstorage.api._
import eu.inn.hyperstorage.recovery.{HotRecoveryWorker, ShutdownRecoveryWorker, StaleRecoveryWorker}
import eu.inn.hyperstorage.sharding.ShardMemberStatus.Active
import eu.inn.hyperstorage.sharding._
import mock.FaultClientTransport
import org.scalatest.concurrent.PatienceConfiguration.{Timeout ⇒ TestTimeout}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FreeSpec, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

// todo: split HyperStorage, shardprocessor and single nodes test
class HyperStorageSpec extends FreeSpec
  with Matchers
  with ScalaFutures
  with CassandraFixture
  with TestHelpers
  with Eventually {

  import ContentLogic._
  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(10000, Millis)))

  "HyperStorage" - {
    "Processor in a single-node cluster" - {
      "ShardProcessor should become Active" in {
        implicit val as = testActorSystem()
        createHyperStorageActor("test-group")
      }

      "ShardProcessor should shutdown gracefully" in {
        implicit val as = testActorSystem()
        val fsm = createHyperStorageActor("test-group")
        shutdownHyperStorageActor(fsm)
      }

      "ShardProcessor should process task" in {
        implicit val as = testActorSystem()
        val fsm = createHyperStorageActor("test-group")
        val task = TestShardTask("abc", "t1")
        fsm ! task
        testKit().awaitCond(task.isProcessed)
        shutdownHyperStorageActor(fsm)
      }

      "ShardProcessor should stash task while Activating and process it later" in {
        implicit val as = testActorSystem()
        val fsm = createHyperStorageActor("test-group", waitWhileActivates = false)
        val task = TestShardTask("abc", "t1")
        fsm ! task
        fsm.stateName should equal(ShardMemberStatus.Activating)
        task.isProcessed should equal(false)
        testKit().awaitCond(task.isProcessed)
        fsm.stateName should equal(ShardMemberStatus.Active)
        shutdownHyperStorageActor(fsm)
      }

      "ShardProcessor should stash task when workers are busy and process later" in {
        implicit val as = testActorSystem()
        val tk = testKit()
        val fsm = createHyperStorageActor("test-group")
        val task1 = TestShardTask("abc1", "t1")
        val task2 = TestShardTask("abc2", "t2")
        fsm ! task1
        fsm ! task2
        tk.awaitCond(task1.isProcessed)
        tk.awaitCond(task2.isProcessed)
        shutdownHyperStorageActor(fsm)
      }

      "ShardProcessor should stash task when URL is 'locked' and it process later" in {
        implicit val as = testActorSystem()
        val tk = testKit()
        val fsm = createHyperStorageActor("test-group", 2)
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
        shutdownHyperStorageActor(fsm)
      }
    }

    "HyperStorage worker" - {
      "Put" in {
        val hyperbus = testHyperbus()
        val tk = testKit()
        import tk._

        cleanUpCassandra()

        val worker = TestActorRef(HyperStorageWorker.props(hyperbus, db, tracker, 10.seconds))

        val task = HyperStorageContentPut(
          path = "test-resource-1",
          DynamicBody(ObjV("text" → "Test resource value", "null" → Null))
        )

        whenReady(db.selectContent("test-resource-1", "")) { result =>
          result shouldBe None
        }

        val taskStr = StringSerializer.serializeToString(task)
        worker ! HyperStorageTask(task.path, System.currentTimeMillis() + 10000, taskStr)
        val completerTask = expectMsgType[CompleterTask]
        completerTask.documentUri should equal(task.path)
        val workerResult = expectMsgType[ShardTaskComplete]
        val r = response(workerResult.result.asInstanceOf[HyperStorageTaskResult].content)
        r.statusCode should equal(Status.CREATED)
        r.correlationId should equal(task.correlationId)

        val uuid = whenReady(db.selectContent("test-resource-1", "")) { result =>
          result.get.body should equal(Some("""{"text":"Test resource value"}"""))
          result.get.transactionList.size should equal(1)

          selectTransactions(result.get.transactionList, result.get.uri, db) foreach { transaction ⇒
            transaction.completedAt shouldBe None
            transaction.revision should equal(result.get.revision)
          }
          result.get.transactionList.head
        }

        val completer = TestActorRef(Completer.props(hyperbus, db, tracker))
        completer ! completerTask
        val completerResult = expectMsgType[ShardTaskComplete]
        val rc = completerResult.result.asInstanceOf[CompleterTaskResult]
        rc.documentUri should equal("test-resource-1")
        rc.transactions should contain(uuid)
        selectTransactions(rc.transactions, "test-resource-1", db) foreach { transaction ⇒
          transaction.completedAt shouldNot be(None)
          transaction.revision should equal(1)
        }
        db.selectContent("test-resource-1", "").futureValue.get.transactionList shouldBe empty
      }

      "Patch resource that doesn't exists" in {
        val hyperbus = testHyperbus()
        val tk = testKit()
        import tk._

        cleanUpCassandra()

        val worker = TestActorRef(HyperStorageWorker.props(hyperbus, db, tracker, 10.seconds))

        val task = HyperStorageContentPatch(
          path = "not-existing",
          DynamicBody(ObjV("text" → "Test resource value"))
        )

        val taskStr = StringSerializer.serializeToString(task)
        worker ! HyperStorageTask(task.path, System.currentTimeMillis() + 10000, taskStr)
        expectMsgPF() {
          case ShardTaskComplete(_, result: HyperStorageTaskResult) if response(result.content).statusCode == Status.NOT_FOUND &&
            response(result.content).correlationId == task.correlationId ⇒ {
            true
          }
        }

        whenReady(db.selectContent("not-existing", "")) { result =>
          result shouldBe None
        }
      }

      "Patch existing and deleted resource" in {
        val hyperbus = testHyperbus()
        val tk = testKit()
        import tk._

        val worker = TestActorRef(HyperStorageWorker.props(hyperbus, db, tracker, 10.seconds))

        val path = "test-resource-" + UUID.randomUUID().toString
        val taskPutStr = StringSerializer.serializeToString(HyperStorageContentPut(path,
          DynamicBody(ObjV("text1" → "abc", "text2" → "klmn"))
        ))

        worker ! HyperStorageTask(path, System.currentTimeMillis() + 10000, taskPutStr)
        expectMsgType[CompleterTask]
        expectMsgType[ShardTaskComplete]

        val task = HyperStorageContentPatch(path,
          DynamicBody(ObjV("text1" → "efg", "text2" → Null, "text3" → "zzz"))
        )
        val taskPatchStr = StringSerializer.serializeToString(task)

        worker ! HyperStorageTask(path, System.currentTimeMillis() + 10000, taskPatchStr)
        expectMsgType[CompleterTask]
        expectMsgPF() {
          case ShardTaskComplete(_, result: HyperStorageTaskResult) if response(result.content).statusCode == Status.OK &&
            response(result.content).correlationId == task.correlationId ⇒ {
            true
          }
        }

        whenReady(db.selectContent(path, "")) { result =>
          result.get.body should equal(Some("""{"text1":"efg","text3":"zzz"}"""))
        }

        // delete element
        val deleteTask = HyperStorageContentDelete(path)
        val deleteTaskStr = StringSerializer.serializeToString(deleteTask)
        worker ! HyperStorageTask(path, System.currentTimeMillis() + 10000, deleteTaskStr)
        expectMsgType[CompleterTask]
        expectMsgType[ShardTaskComplete]

        // now patch should return 404
        worker ! HyperStorageTask(path, System.currentTimeMillis() + 10000, taskPatchStr)

        expectMsgPF() {
          case ShardTaskComplete(_, result: HyperStorageTaskResult) if response(result.content).statusCode == Status.NOT_FOUND &&
            response(result.content).correlationId == task.correlationId ⇒ {
            true
          }
        }
      }

      "Delete resource that doesn't exists" in {
        val hyperbus = testHyperbus()
        val tk = testKit()
        import tk._

        cleanUpCassandra()

        val worker = TestActorRef(HyperStorageWorker.props(hyperbus, db, tracker, 10.seconds))

        val task = HyperStorageContentDelete(path = "not-existing", body = EmptyBody)

        val taskStr = StringSerializer.serializeToString(task)
        worker ! HyperStorageTask(task.path, System.currentTimeMillis() + 10000, taskStr)
        expectMsgPF() {
          case ShardTaskComplete(_, result: HyperStorageTaskResult) if response(result.content).statusCode == Status.NOT_FOUND &&
            response(result.content).correlationId == task.correlationId ⇒ {
            true
          }
        }

        whenReady(db.selectContent("not-existing", "")) { result =>
          result shouldBe None
        }
      }

      "Delete resource that exists" in {
        val hyperbus = testHyperbus()
        val tk = testKit()
        import tk._

        cleanUpCassandra()

        val worker = TestActorRef(HyperStorageWorker.props(hyperbus, db, tracker, 10.seconds))

        val path = "test-resource-" + UUID.randomUUID().toString
        val taskPutStr = StringSerializer.serializeToString(HyperStorageContentPut(path,
          DynamicBody(ObjV("text1" → "abc", "text2" → "klmn"))
        ))

        worker ! HyperStorageTask(path, System.currentTimeMillis() + 10000, taskPutStr)
        expectMsgType[CompleterTask]
        expectMsgType[ShardTaskComplete]

        whenReady(db.selectContent(path, "")) { result =>
          result shouldNot be(None)
        }

        val task = HyperStorageContentDelete(path)

        val taskStr = StringSerializer.serializeToString(task)
        worker ! HyperStorageTask(path, System.currentTimeMillis() + 10000, taskStr)
        expectMsgType[CompleterTask]
        expectMsgPF() {
          case ShardTaskComplete(_, result: HyperStorageTaskResult) if response(result.content).statusCode == Status.OK &&
            response(result.content).correlationId == task.correlationId ⇒ {
            true
          }
        }

        whenReady(db.selectContent(path, "")) { result =>
          result.get.isDeleted shouldBe true
        }
      }

      "Test multiple transactions" in {
        val hyperbus = testHyperbus()
        val tk = testKit()
        import tk._

        cleanUpCassandra()

        val worker = TestActorRef(HyperStorageWorker.props(hyperbus, db, tracker, 10.seconds))
        val path = "abcde"
        val taskStr1 = StringSerializer.serializeToString(HyperStorageContentPut(path,
          DynamicBody(ObjV("text" → "Test resource value", "null" → Null))
        ))
        worker ! HyperStorageTask(path, System.currentTimeMillis() + 10000, taskStr1)
        expectMsgType[CompleterTask]
        expectMsgType[ShardTaskComplete]

        val taskStr2 = StringSerializer.serializeToString(HyperStorageContentPatch(path,
          DynamicBody(ObjV("text" → "abc", "text2" → "klmn"))
        ))
        worker ! HyperStorageTask(path, System.currentTimeMillis() + 10000, taskStr2)
        expectMsgType[CompleterTask]
        expectMsgType[ShardTaskComplete]

        val taskStr3 = StringSerializer.serializeToString(HyperStorageContentDelete(path))
        worker ! HyperStorageTask(path, System.currentTimeMillis() + 10000, taskStr3)
        val completerTask = expectMsgType[CompleterTask]
        val workerResult = expectMsgType[ShardTaskComplete]
        val r = response(workerResult.result.asInstanceOf[HyperStorageTaskResult].content)
        r.statusCode should equal(Status.OK)

        val transactions = whenReady(db.selectContent(path, "")) { result =>
          result.get.isDeleted should equal(true)
          result.get.transactionList
        }

        selectTransactions(transactions, path, db) foreach { transaction ⇒
          transaction.completedAt should be(None)
        }

        val completer = TestActorRef(Completer.props(hyperbus, db, tracker))
        completer ! completerTask
        val completerResult = expectMsgType[ShardTaskComplete]
        val rc = completerResult.result.asInstanceOf[CompleterTaskResult]
        rc.documentUri should equal(path)
        rc.transactions should equal(transactions.reverse)

        selectTransactions(rc.transactions, path, db) foreach { transaction ⇒
          transaction.completedAt shouldNot be(None)
        }
      }

      "Test faulty publish" in {
        val hyperbus = testHyperbus()
        val tk = testKit()
        import tk._

        cleanUpCassandra()

        val worker = TestActorRef(HyperStorageWorker.props(hyperbus, db, tracker, 10.seconds))
        val path = "faulty"
        val taskStr1 = StringSerializer.serializeToString(HyperStorageContentPut(path,
          DynamicBody(ObjV("text" → "Test resource value", "null" → Null))
        ))
        worker ! HyperStorageTask(path, System.currentTimeMillis() + 10000, taskStr1)
        expectMsgType[CompleterTask]
        expectMsgType[ShardTaskComplete]

        val taskStr2 = StringSerializer.serializeToString(HyperStorageContentPatch(path,
          DynamicBody(ObjV("text" → "abc", "text2" → "klmn"))
        ))
        worker ! HyperStorageTask(path, System.currentTimeMillis() + 10000, taskStr2)
        val completerTask = expectMsgType[CompleterTask]
        expectMsgType[ShardTaskComplete]

        val transactionUuids = whenReady(db.selectContent(path, "")) { result =>
          result.get.transactionList
        }

        selectTransactions(transactionUuids, path, db) foreach {
          _.completedAt shouldBe None
        }

        val completer = TestActorRef(Completer.props(hyperbus, db, tracker))

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
        selectTransactions(transactionUuids, path, db) foreach {
          _.completedAt shouldBe None
        }

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
        val mons = selectTransactions(transactionUuids, path, db)

        mons.head.completedAt shouldBe None
        mons.tail.head.completedAt shouldNot be(None)

        FaultClientTransport.checkers.clear()
        completer ! completerTask
        expectMsgType[ShardTaskComplete].result shouldBe a[CompleterTaskResult]
        selectTransactions(transactionUuids, path, db) foreach {
          _.completedAt shouldNot be(None)
        }

        db.selectContent(path, "").futureValue.get.transactionList shouldBe empty
      }
    }

    "HyperStorage worker (collections)" - {
      "Put item" in {
        val hyperbus = testHyperbus()
        val tk = testKit()
        import tk._

        cleanUpCassandra()

        val worker = TestActorRef(HyperStorageWorker.props(hyperbus, db, tracker, 10.seconds))

        val task = HyperStorageContentPut(
          path = "collection-1~/test-resource-1",
          DynamicBody(ObjV("text" → "Test item value", "null" → Null))
        )

        db.selectContent("collection-1~", "test-resource-1").futureValue shouldBe None

        val taskStr = StringSerializer.serializeToString(task)
        worker ! HyperStorageTask("collection-1~", System.currentTimeMillis() + 10000, taskStr)
        val completerTask = expectMsgType[CompleterTask]
        completerTask.documentUri should equal("collection-1~")
        val workerResult = expectMsgType[ShardTaskComplete]
        val r = response(workerResult.result.asInstanceOf[HyperStorageTaskResult].content)
        r.statusCode should equal(Status.CREATED)
        r.correlationId should equal(task.correlationId)

        val content = db.selectContent("collection-1~", "test-resource-1").futureValue
        content shouldNot equal(None)
        content.get.body should equal(Some("""{"text":"Test item value","id":"test-resource-1"}"""))
        content.get.transactionList.size should equal(1)
        content.get.revision should equal(1)
        val uuid = content.get.transactionList.head

        val task2 = HyperStorageContentPut(
          path = "collection-1~/test-resource-2",
          DynamicBody(ObjV("text" → "Test item value 2"))
        )
        val task2Str = StringSerializer.serializeToString(task2)
        worker ! HyperStorageTask("collection-1~", System.currentTimeMillis() + 10000, task2Str)
        val completerTask2 = expectMsgType[CompleterTask]
        completerTask2.documentUri should equal("collection-1~")
        val workerResult2 = expectMsgType[ShardTaskComplete]
        val r2 = response(workerResult2.result.asInstanceOf[HyperStorageTaskResult].content)
        r2.statusCode should equal(Status.CREATED)
        r2.correlationId should equal(task2.correlationId)

        val content2 = db.selectContent("collection-1~", "test-resource-2").futureValue
        content2 shouldNot equal(None)
        content2.get.body should equal(Some("""{"text":"Test item value 2","id":"test-resource-2"}"""))
        content2.get.transactionList.size should equal(2)
        content2.get.revision should equal(2)

        val transactions = selectTransactions(content2.get.transactionList, "collection-1~", db)
        transactions.size should equal(2)
        transactions.foreach {_.completedAt shouldBe None}
        transactions.head.revision should equal(2)
        transactions.tail.head.revision should equal(1)

        val completer = TestActorRef(Completer.props(hyperbus, db, tracker))
        completer ! completerTask
        val completerResult = expectMsgType[ShardTaskComplete]
        val rc = completerResult.result.asInstanceOf[CompleterTaskResult]
        rc.documentUri should equal("collection-1~")
        rc.transactions should equal(content2.get.transactionList.reverse)

        eventually {
          db.selectContentStatic("collection-1~").futureValue.get.transactionList shouldBe empty
        }
        selectTransactions(content2.get.transactionList, "collection-1~", db).foreach {_.completedAt shouldNot be(None)}
      }

      "Patch item" in {
        val hyperbus = testHyperbus()
        val tk = testKit()
        import tk._

        val worker = TestActorRef(HyperStorageWorker.props(hyperbus, db, tracker, 10.seconds))

        val path = "collection-1~/test-resource-" + UUID.randomUUID().toString
        val ResourcePath(documentUri,itemSegment) = ContentLogic.splitPath(path)

        val taskPutStr = StringSerializer.serializeToString(HyperStorageContentPut(path,
          DynamicBody(ObjV("text1" → "abc", "text2" → "klmn"))
        ))

        worker ! HyperStorageTask(documentUri, System.currentTimeMillis() + 10000, taskPutStr)
        expectMsgType[CompleterTask]
        expectMsgType[ShardTaskComplete]

        val task = HyperStorageContentPatch(path,
          DynamicBody(ObjV("text1" → "efg", "text2" → Null, "text3" → "zzz"))
        )
        val taskPatchStr = StringSerializer.serializeToString(task)
        worker ! HyperStorageTask(documentUri, System.currentTimeMillis() + 10000, taskPatchStr)

        expectMsgType[CompleterTask]
        expectMsgPF() {
          case ShardTaskComplete(_, result: HyperStorageTaskResult) if response(result.content).statusCode == Status.OK &&
            response(result.content).correlationId == task.correlationId ⇒ {
            true
          }
        }

        whenReady(db.selectContent(documentUri, itemSegment)) { result =>
          result.get.body should equal(Some(s"""{"text1":"efg","id":"$itemSegment","text3":"zzz"}"""))
          result.get.modifiedAt shouldNot be(None)
          result.get.documentUri should equal(documentUri)
          result.get.itemSegment should equal(itemSegment)
        }

        // delete element
        val deleteTask = HyperStorageContentDelete(path)
        val deleteTaskStr = StringSerializer.serializeToString(deleteTask)
        worker ! HyperStorageTask(documentUri, System.currentTimeMillis() + 10000, deleteTaskStr)
        expectMsgType[CompleterTask]
        expectMsgType[ShardTaskComplete]

        // now patch should return 404
        worker ! HyperStorageTask(documentUri, System.currentTimeMillis() + 10000, taskPatchStr)

        expectMsgPF() {
          case ShardTaskComplete(_, result: HyperStorageTaskResult) if response(result.content).statusCode == Status.NOT_FOUND &&
            response(result.content).correlationId == task.correlationId ⇒ {
            true
          }
        }
      }

      "Delete item" in {
        val hyperbus = testHyperbus()
        val tk = testKit()
        import tk._

        val worker = TestActorRef(HyperStorageWorker.props(hyperbus, db, tracker, 10.seconds))

        val path = "collection-1~/test-resource-" + UUID.randomUUID().toString
        val ResourcePath(documentUri,itemSegment) = ContentLogic.splitPath(path)

        val taskPutStr = StringSerializer.serializeToString(HyperStorageContentPut(path,
          DynamicBody(ObjV("text1" → "abc", "text2" → "klmn"))
        ))

        worker ! HyperStorageTask(documentUri, System.currentTimeMillis() + 10000, taskPutStr)
        expectMsgType[CompleterTask]
        expectMsgType[ShardTaskComplete]

        val task = HyperStorageContentDelete(path)
        val taskStr = StringSerializer.serializeToString(task)
        worker ! HyperStorageTask(documentUri, System.currentTimeMillis() + 10000, taskStr)

        expectMsgType[CompleterTask]
        expectMsgPF() {
          case ShardTaskComplete(_, result: HyperStorageTaskResult) if response(result.content).statusCode == Status.OK &&
            response(result.content).correlationId == task.correlationId ⇒ {
            true
          }
        }

        whenReady(db.selectContent(documentUri, itemSegment)) { result =>
          result.get.isDeleted shouldNot be(None)
          result.get.modifiedAt shouldNot be(None)
        }
      }
    }

    "HyperStorage integrated" - {
      "Test hyper-storage PUT+GET+Event" in {
        val hyperbus = testHyperbus()
        val tk = testKit()
        import tk._
        import system._

        cleanUpCassandra()

        val workerProps = HyperStorageWorker.props(hyperbus, db, tracker, 10.seconds)
        val completerProps = Completer.props(hyperbus, db, tracker)
        val workerSettings = Map(
          "hyper-storage" →(workerProps, 1),
          "hyper-storage-completer" →(completerProps, 1)
        )

        val processor = TestActorRef(ShardProcessor.props(workerSettings, "hyper-storage", tracker))
        val distributor = TestActorRef(HyperbusAdapter.props(processor, db, tracker, 20.seconds))
        import eu.inn.hyperbus.akkaservice._
        implicit val timeout = Timeout(20.seconds)
        hyperbus.routeTo[HyperbusAdapter](distributor).futureValue // wait while subscription is completes

        val putEventPromise = Promise[HyperStorageContentFeedPut]()
        hyperbus |> { put: HyperStorageContentFeedPut ⇒
          Future {
            putEventPromise.success(put)
          }
        }

        Thread.sleep(2000)

        val path = UUID.randomUUID().toString
        val f1 = hyperbus <~ HyperStorageContentPut(path, DynamicBody(Text("Hello")))
        whenReady(f1) { response ⇒
          response.statusCode should equal(Status.CREATED)
        }

        val putEventFuture = putEventPromise.future
        whenReady(putEventFuture) { putEvent ⇒
          putEvent.method should equal(Method.FEED_PUT)
          putEvent.body should equal(DynamicBody(Text("Hello")))
          putEvent.headers.get(Header.REVISION) shouldNot be(None)
        }

        whenReady(hyperbus <~ HyperStorageContentGet(path), TestTimeout(10.seconds)) { response ⇒
          response.statusCode should equal(Status.OK)
          response.body.content should equal(Text("Hello"))
        }
      }

      "Null patch with hyper-storage (integrated)" in {
        val hyperbus = testHyperbus()
        val tk = testKit()
        import tk._
        import system._

        cleanUpCassandra()

        val workerProps = HyperStorageWorker.props(hyperbus, db, tracker, 10.seconds)
        val completerProps = Completer.props(hyperbus, db, tracker)
        val workerSettings = Map(
          "hyper-storage" →(workerProps, 1),
          "hyper-storage-completer" →(completerProps, 1)
        )

        val processor = TestActorRef(ShardProcessor.props(workerSettings, "hyper-storage", tracker))
        val distributor = TestActorRef(HyperbusAdapter.props(processor, db, tracker, 20.seconds))
        import eu.inn.hyperbus.akkaservice._
        implicit val timeout = Timeout(20.seconds)
        hyperbus.routeTo[HyperbusAdapter](distributor)

        val patchEventPromise = Promise[HyperStorageContentFeedPatch]()
        hyperbus |> { patch: HyperStorageContentFeedPatch ⇒
          Future {
            patchEventPromise.success(patch)
          }
        }

        Thread.sleep(2000)

        val path = UUID.randomUUID().toString
        whenReady(hyperbus <~ HyperStorageContentPut(path, DynamicBody(
          ObjV("a" → "1", "b" → "2", "c" → "3")
        )), TestTimeout(10.seconds)) { response ⇒
          response.statusCode should equal(Status.CREATED)
        }

        val f = hyperbus <~ HyperStorageContentPatch(path, DynamicBody(ObjV("b" → Null)))
        whenReady(f) { response ⇒
          response.statusCode should equal(Status.OK)
        }

        val patchEventFuture = patchEventPromise.future
        whenReady(patchEventFuture) { patchEvent ⇒
          patchEvent.method should equal(Method.FEED_PATCH)
          patchEvent.body should equal(DynamicBody(ObjV("b" → Null)))
          patchEvent.headers.get(Header.REVISION) shouldNot be(None)
        }

        whenReady(hyperbus <~ HyperStorageContentGet(path), TestTimeout(10.seconds)) { response ⇒
          response.statusCode should equal(Status.OK)
          response.body.content should equal(ObjV("a" → "1", "c" → "3"))
        }
      }

      "Test hyper-storage PUT+GET+GET Collection+Event" in {
        val hyperbus = testHyperbus()
        val tk = testKit()
        import tk._
        import system._

        cleanUpCassandra()

        val workerProps = HyperStorageWorker.props(hyperbus, db, tracker, 10.seconds)
        val completerProps = Completer.props(hyperbus, db, tracker)
        val workerSettings = Map(
          "hyper-storage" →(workerProps, 1),
          "hyper-storage-completer" →(completerProps, 1)
        )

        val processor = TestActorRef(ShardProcessor.props(workerSettings, "hyper-storage", tracker))
        val distributor = TestActorRef(HyperbusAdapter.props(processor, db, tracker, 20.seconds))
        import eu.inn.hyperbus.akkaservice._
        implicit val timeout = Timeout(20.seconds)
        hyperbus.routeTo[HyperbusAdapter](distributor).futureValue // wait while subscription is completes

        val putEventPromise = Promise[HyperStorageContentFeedPut]()
        hyperbus |> { put: HyperStorageContentFeedPut ⇒
          Future {
            if (!putEventPromise.isCompleted) {
              putEventPromise.success(put)
            }
          }
        }

        Thread.sleep(2000)

        val c1 = ObjV("a" → "hello", "b" → 100500)
        val c2 = ObjV("a" → "good by", "b" → 654321)
        val c1x = Obj(c1.asMap + "id" → "item1")
        val c2x = Obj(c2.asMap + "id" → "item2")

        val path = "collection-1~/item1"
        val f = hyperbus <~ HyperStorageContentPut(path, DynamicBody(c1))
        whenReady(f) { case response: Response[Body] ⇒
          response.statusCode should equal(Status.CREATED)
        }

        val putEventFuture = putEventPromise.future
        whenReady(putEventFuture) { putEvent ⇒
          putEvent.method should equal(Method.FEED_PUT)
          putEvent.body should equal(DynamicBody(c1x))
          putEvent.headers.get(Header.REVISION) shouldNot be(None)
        }

        val f2 = hyperbus <~ HyperStorageContentGet(path)
        whenReady(f2) { response ⇒
          response.statusCode should equal(Status.OK)
          response.body.content should equal(c1x)
        }

        val path2 = "collection-1~/item2"
        val f3 = hyperbus <~ HyperStorageContentPut(path2, DynamicBody(c2x))
        whenReady(f3) { response ⇒
          response.statusCode should equal(Status.CREATED)
        }

        val f4 = hyperbus <~ HyperStorageContentGet("collection-1~",
          body = new QueryBuilder() add("ct", Null) result())

        whenReady(f4) { response ⇒
          response.statusCode should equal(Status.OK)
          response.body.content should equal(
            ObjV("_embedded" -> ObjV("els" → LstV(c1x, c2x)))
          )
        }

        import Sort._

        val f5 = hyperbus <~ HyperStorageContentGet("collection-1~",
          body = new QueryBuilder() add("ct", Null) sortBy(Seq(SortBy("id", true))) result())

        whenReady(f5) { response ⇒
          response.statusCode should equal(Status.OK)
          response.body.content should equal(
            ObjV("_embedded" -> ObjV("els" → LstV(c2x, c1x)))
          )
        }
      }

      "Test hyper-storage POST+GET+GET Collection+Event" in {
        val hyperbus = testHyperbus()
        val tk = testKit()
        import tk._
        import system._

        cleanUpCassandra()

        val workerProps = HyperStorageWorker.props(hyperbus, db, tracker, 10.seconds)
        val completerProps = Completer.props(hyperbus, db, tracker)
        val workerSettings = Map(
          "hyper-storage" →(workerProps, 1),
          "hyper-storage-completer" →(completerProps, 1)
        )

        val processor = TestActorRef(ShardProcessor.props(workerSettings, "hyper-storage", tracker))
        val distributor = TestActorRef(HyperbusAdapter.props(processor, db, tracker, 20.seconds))
        import eu.inn.hyperbus.akkaservice._
        implicit val timeout = Timeout(20.seconds)
        hyperbus.routeTo[HyperbusAdapter](distributor).futureValue // wait while subscription is completes

        val putEventPromise = Promise[HyperStorageContentFeedPut]()
        hyperbus |> { put: HyperStorageContentFeedPut ⇒
          Future {
            if (!putEventPromise.isCompleted) {
              putEventPromise.success(put)
            }
          }
        }

        Thread.sleep(2000)

        val c1 = ObjV("a" → "hello", "b" → Number(100500))
        val c2 = ObjV("a" → "good by", "b" → Number(654321))

        val path = "collection-2~"
        val f = hyperbus <~ HyperStorageContentPost(path, DynamicBody(c1))
        val tr1: HyperStorageTransactionCreated = whenReady(f) { case response: Created[HyperStorageTransactionCreated] ⇒
          response.statusCode should equal(Status.CREATED)
          response.body
        }

        val id1 = tr1.path.split('/').tail.head
        val c1x = Obj(c1.asMap + "id" → id1)

        val putEventFuture = putEventPromise.future
        whenReady(putEventFuture) { putEvent ⇒
          putEvent.method should equal(Method.FEED_PUT)
          putEvent.body should equal(DynamicBody(c1x))
          putEvent.headers.get(Header.REVISION) shouldNot be(None)
        }

        val f2 = hyperbus <~ HyperStorageContentGet(tr1.path)
        whenReady(f2) { response ⇒
          response.statusCode should equal(Status.OK)
          response.body.content should equal(c1x)
        }

        val f3 = hyperbus <~ HyperStorageContentPost(path, DynamicBody(c2))
        val tr2: HyperStorageTransactionCreated = whenReady(f3) { case response: Created[CreatedBody] ⇒
          response.statusCode should equal(Status.CREATED)
          response.body
        }

        val id2 = tr2.path.split('/').tail.head
        val c2x = Obj(c2.asMap + "id" → id2)

        val f4 = hyperbus <~ HyperStorageContentGet("collection-2~",
          body = new QueryBuilder() add("ct", Null) result())

        whenReady(f4) { response ⇒
          response.statusCode should equal(Status.OK)
          response.body.content should equal(
            ObjV("_embedded" -> ObjV("els" → LstV(c1x,c2x)))
          )
        }

        import Sort._

        val f5 = hyperbus <~ HyperStorageContentGet("collection-2~",
          body = new QueryBuilder() add("ct", Null) sortBy(Seq(SortBy("id", true))) result())
        whenReady(f5) { response ⇒
          response.statusCode should equal(Status.OK)
          response.body.content should equal(
            ObjV("_embedded" -> ObjV("els" → LstV(c2x,c1x)))
          )
        }
      }
    }

    "Recovery" - {
      "HotRecoveryWorker" in {
        val hyperbus = testHyperbus()
        val tk = testKit()
        import tk._

        cleanUpCassandra()

        val worker = TestActorRef(HyperStorageWorker.props(hyperbus, db, tracker, 10.seconds))
        val path = "incomplete-" + UUID.randomUUID().toString
        val taskStr1 = StringSerializer.serializeToString(HyperStorageContentPut(path,
          DynamicBody(ObjV("text" → "Test resource value", "null" → Null))
        ))
        worker ! HyperStorageTask(path, System.currentTimeMillis() + 10000, taskStr1)
        val completerTask = expectMsgType[CompleterTask]
        expectMsgType[ShardTaskComplete]

        val transactionUuids = whenReady(db.selectContent(path, "")) { result =>
          result.get.transactionList
        }

        val processorProbe = TestProbe("processor")
        val hotWorkerProps = HotRecoveryWorker.props(
          (60 * 1000l, -60 * 1000l), db, processorProbe.ref, tracker, 1.seconds, 10.seconds
        )

        val hotWorker = TestActorRef(hotWorkerProps)
        val selfAddress = Address("tcp", "127.0.0.1")
        val shardData = ShardedClusterData(Map(
          selfAddress → ShardMember(ActorSelection(self, ""), ShardMemberStatus.Active, ShardMemberStatus.Active)
        ), selfAddress, ShardMemberStatus.Active)

        // start recovery check
        hotWorker ! UpdateShardStatus(self, Active, shardData)

        val completerTask2 = processorProbe.expectMsgType[CompleterTask](max = 30.seconds)
        completerTask.documentUri should equal(completerTask2.documentUri)
        processorProbe.reply(CompletionFailedException(completerTask2.documentUri, "Testing worker behavior"))
        val completerTask3 = processorProbe.expectMsgType[CompleterTask](max = 30.seconds)
        hotWorker ! processorProbe.reply(CompleterTaskResult(completerTask2.documentUri, transactionUuids))
        gracefulStop(hotWorker, 30 seconds, ShutdownRecoveryWorker).futureValue(TestTimeout(30.seconds))
      }

      "StaleRecoveryWorker" in {
        val hyperbus = testHyperbus()
        val tk = testKit()
        import tk._

        cleanUpCassandra()

        val worker = TestActorRef(HyperStorageWorker.props(hyperbus, db, tracker, 10.seconds))
        val path = "incomplete-" + UUID.randomUUID().toString
        val taskStr1 = StringSerializer.serializeToString(HyperStorageContentPut(path,
          DynamicBody(ObjV("text" → "Test resource value", "null" → Null))
        ))
        val millis = System.currentTimeMillis()
        worker ! HyperStorageTask(path, System.currentTimeMillis() + 10000, taskStr1)
        val completerTask = expectMsgType[CompleterTask]
        expectMsgType[ShardTaskComplete]

        val content = db.selectContent(path, "").futureValue.get
        val newTransactionUuid = UUIDs.startOf(millis - 5 * 60 * 1000l)
        val newContent = content.copy(
          transactionList = List(newTransactionUuid) // original transaction becomes abandoned
        )
        val transaction = selectTransactions(content.transactionList, content.uri, db).head
        val newTransaction = transaction.copy(
          dtQuantum = TransactionLogic.getDtQuantum(UUIDs.unixTimestamp(newTransactionUuid)),
          uuid = newTransactionUuid
        )
        db.insertContent(newContent).futureValue
        db.insertTransaction(newTransaction).futureValue
        println(s"old uuid = ${transaction.uuid}, new uuid = $newTransactionUuid, ${UUIDs.unixTimestamp(transaction.uuid)}, ${UUIDs.unixTimestamp(newTransactionUuid)}")

        db.updateCheckpoint(transaction.partition, transaction.dtQuantum - 10) // checkpoint to - 10 minutes

        val processorProbe = TestProbe("processor")
        val staleWorkerProps = StaleRecoveryWorker.props(
          (60 * 1000l, -60 * 1000l), db, processorProbe.ref, tracker, 1.seconds, 2.seconds
        )

        val hotWorker = TestActorRef(staleWorkerProps)
        val selfAddress = Address("tcp", "127.0.0.1")
        val shardData = ShardedClusterData(Map(
          selfAddress → ShardMember(ActorSelection(self, ""), ShardMemberStatus.Active, ShardMemberStatus.Active)
        ), selfAddress, ShardMemberStatus.Active)

        // start recovery check
        hotWorker ! UpdateShardStatus(self, Active, shardData)

        val completerTask2 = processorProbe.expectMsgType[CompleterTask](max = 30.seconds)
        completerTask.documentUri should equal(completerTask2.documentUri)
        processorProbe.reply(CompletionFailedException(completerTask2.documentUri, "Testing worker behavior"))

        eventually {
          db.selectCheckpoint(transaction.partition).futureValue shouldBe Some(newTransaction.dtQuantum - 1)
        }

        val completerTask3 = processorProbe.expectMsgType[CompleterTask](max = 30.seconds)
        hotWorker ! processorProbe.reply(CompleterTaskResult(completerTask2.documentUri, newContent.transactionList))

        eventually {
          db.selectCheckpoint(transaction.partition).futureValue.get shouldBe >(newTransaction.dtQuantum)
        }

        val completerTask4 = processorProbe.expectMsgType[CompleterTask](max = 30.seconds) // this is abandoned
        hotWorker ! processorProbe.reply(CompleterTaskResult(completerTask4.documentUri, List()))

        gracefulStop(hotWorker, 30 seconds, ShutdownRecoveryWorker).futureValue(TestTimeout(30.seconds))
      }
    }
  }
  def response(content: String): Response[Body] = StringDeserializer.dynamicResponse(content)
}
