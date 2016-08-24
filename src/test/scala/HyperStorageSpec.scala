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
import eu.inn.hyperstorage.db.IndexDef
import eu.inn.hyperstorage.recovery.{HotRecoveryWorker, ShutdownRecoveryWorker, StaleRecoveryWorker}
import eu.inn.hyperstorage.sharding.ShardMemberStatus.Active
import eu.inn.hyperstorage.sharding._
import mock.FaultClientTransport
import org.scalatest.concurrent.PatienceConfiguration.{Timeout ⇒ TestTimeout}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FreeSpec, Matchers}

import scala.collection.mutable
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

    "HyperStorage worker" - {
      "Put" in {
        val hyperbus = testHyperbus()
        val tk = testKit()
        import tk._

        cleanUpCassandra()

        val worker = TestActorRef(ForegroundWorker.props(hyperbus, db, tracker, 10.seconds))

        val task = HyperStorageContentPut(
          path = "test-resource-1",
          DynamicBody(ObjV("text" → "Test resource value", "null" → Null))
        )

        whenReady(db.selectContent("test-resource-1", "")) { result =>
          result shouldBe None
        }

        val taskStr = StringSerializer.serializeToString(task)
        worker ! ForegroundTask(task.path, System.currentTimeMillis() + 10000, taskStr)
        val backgroundTask = expectMsgType[BackgroundTask]
        backgroundTask.documentUri should equal(task.path)
        val workerResult = expectMsgType[ShardTaskComplete]
        val r = response(workerResult.result.asInstanceOf[ForegroundWorkerTaskResult].content)
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

        val backgroundWorker = TestActorRef(BackgroundWorker.props(hyperbus, db, tracker, self))
        backgroundWorker ! backgroundTask
        val backgroundWorkerResult = expectMsgType[ShardTaskComplete]
        val rc = backgroundWorkerResult.result.asInstanceOf[BackgroundTaskResult]
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

        val worker = TestActorRef(ForegroundWorker.props(hyperbus, db, tracker, 10.seconds))

        val task = HyperStorageContentPatch(
          path = "not-existing",
          DynamicBody(ObjV("text" → "Test resource value"))
        )

        val taskStr = StringSerializer.serializeToString(task)
        worker ! ForegroundTask(task.path, System.currentTimeMillis() + 10000, taskStr)
        expectMsgPF() {
          case ShardTaskComplete(_, result: ForegroundWorkerTaskResult) if response(result.content).statusCode == Status.NOT_FOUND &&
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

        val worker = TestActorRef(ForegroundWorker.props(hyperbus, db, tracker, 10.seconds))

        val path = "test-resource-" + UUID.randomUUID().toString
        val taskPutStr = StringSerializer.serializeToString(HyperStorageContentPut(path,
          DynamicBody(ObjV("text1" → "abc", "text2" → "klmn"))
        ))

        worker ! ForegroundTask(path, System.currentTimeMillis() + 10000, taskPutStr)
        expectMsgType[BackgroundTask]
        expectMsgType[ShardTaskComplete]

        val task = HyperStorageContentPatch(path,
          DynamicBody(ObjV("text1" → "efg", "text2" → Null, "text3" → "zzz"))
        )
        val taskPatchStr = StringSerializer.serializeToString(task)

        worker ! ForegroundTask(path, System.currentTimeMillis() + 10000, taskPatchStr)
        expectMsgType[BackgroundTask]
        expectMsgPF() {
          case ShardTaskComplete(_, result: ForegroundWorkerTaskResult) if response(result.content).statusCode == Status.OK &&
            response(result.content).correlationId == task.correlationId ⇒ {
            true
          }
        }

        whenReady(db.selectContent(path, "")) { result =>
          result.get.body should equal(Some("""{"text1":"efg","text3":"zzz"}"""))
        }

        // delete resource
        val deleteTask = HyperStorageContentDelete(path)
        val deleteTaskStr = StringSerializer.serializeToString(deleteTask)
        worker ! ForegroundTask(path, System.currentTimeMillis() + 10000, deleteTaskStr)
        expectMsgType[BackgroundTask]
        expectMsgType[ShardTaskComplete]

        // now patch should return 404
        worker ! ForegroundTask(path, System.currentTimeMillis() + 10000, taskPatchStr)

        expectMsgPF() {
          case ShardTaskComplete(_, result: ForegroundWorkerTaskResult) if response(result.content).statusCode == Status.NOT_FOUND &&
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

        val worker = TestActorRef(ForegroundWorker.props(hyperbus, db, tracker, 10.seconds))

        val task = HyperStorageContentDelete(path = "not-existing", body = EmptyBody)

        val taskStr = StringSerializer.serializeToString(task)
        worker ! ForegroundTask(task.path, System.currentTimeMillis() + 10000, taskStr)
        expectMsgPF() {
          case ShardTaskComplete(_, result: ForegroundWorkerTaskResult) if response(result.content).statusCode == Status.NOT_FOUND &&
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

        val worker = TestActorRef(ForegroundWorker.props(hyperbus, db, tracker, 10.seconds))

        val path = "test-resource-" + UUID.randomUUID().toString
        val taskPutStr = StringSerializer.serializeToString(HyperStorageContentPut(path,
          DynamicBody(ObjV("text1" → "abc", "text2" → "klmn"))
        ))

        worker ! ForegroundTask(path, System.currentTimeMillis() + 10000, taskPutStr)
        expectMsgType[BackgroundTask]
        expectMsgType[ShardTaskComplete]

        whenReady(db.selectContent(path, "")) { result =>
          result shouldNot be(None)
        }

        val task = HyperStorageContentDelete(path)

        val taskStr = StringSerializer.serializeToString(task)
        worker ! ForegroundTask(path, System.currentTimeMillis() + 10000, taskStr)
        expectMsgType[BackgroundTask]
        expectMsgPF() {
          case ShardTaskComplete(_, result: ForegroundWorkerTaskResult) if response(result.content).statusCode == Status.OK &&
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

        val transactionList = mutable.ListBuffer[Value]()

        val worker = TestActorRef(ForegroundWorker.props(hyperbus, db, tracker, 10.seconds))
        val path = "abcde"
        val taskStr1 = StringSerializer.serializeToString(HyperStorageContentPut(path,
          DynamicBody(ObjV("text" → "Test resource value", "null" → Null))
        ))
        worker ! ForegroundTask(path, System.currentTimeMillis() + 10000, taskStr1)
        expectMsgType[BackgroundTask]
        val result1 = expectMsgType[ShardTaskComplete]
        transactionList += response(result1.result.asInstanceOf[ForegroundWorkerTaskResult].content).body.content.transactionId

        val taskStr2 = StringSerializer.serializeToString(HyperStorageContentPatch(path,
          DynamicBody(ObjV("text" → "abc", "text2" → "klmn"))
        ))
        worker ! ForegroundTask(path, System.currentTimeMillis() + 10000, taskStr2)
        expectMsgType[BackgroundTask]
        val result2 = expectMsgType[ShardTaskComplete]
        transactionList += response(result2.result.asInstanceOf[ForegroundWorkerTaskResult].content).body.content.transactionId

        val taskStr3 = StringSerializer.serializeToString(HyperStorageContentDelete(path))
        worker ! ForegroundTask(path, System.currentTimeMillis() + 10000, taskStr3)
        val backgroundWorkerTask = expectMsgType[BackgroundTask]
        val workerResult = expectMsgType[ShardTaskComplete]
        val r = response(workerResult.result.asInstanceOf[ForegroundWorkerTaskResult].content)
        r.statusCode should equal(Status.OK)
        transactionList += r.body.content.transactionId
        // todo: list of transactions!s

        val transactionsC = whenReady(db.selectContent(path, "")) { result =>
          result.get.isDeleted should equal(true)
          result.get.transactionList
        }

        val transactions = transactionList.map(s ⇒ UUID.fromString(s.asString.split(':')(1)))

        transactions should equal(transactionsC.reverse)

        selectTransactions(transactions, path, db) foreach { transaction ⇒
          transaction.completedAt should be(None)
        }

        val backgroundWorker = TestActorRef(BackgroundWorker.props(hyperbus, db, tracker, self))
        backgroundWorker ! backgroundWorkerTask
        val backgroundWorkerResult = expectMsgType[ShardTaskComplete]
        val rc = backgroundWorkerResult.result.asInstanceOf[BackgroundTaskResult]
        rc.documentUri should equal(path)
        rc.transactions should equal(transactions)

        selectTransactions(rc.transactions, path, db) foreach { transaction ⇒
          transaction.completedAt shouldNot be(None)
        }
      }

      "Test faulty publish" in {
        val hyperbus = testHyperbus()
        val tk = testKit()
        import tk._

        cleanUpCassandra()

        val worker = TestActorRef(ForegroundWorker.props(hyperbus, db, tracker, 10.seconds))
        val path = "faulty"
        val taskStr1 = StringSerializer.serializeToString(HyperStorageContentPut(path,
          DynamicBody(ObjV("text" → "Test resource value", "null" → Null))
        ))
        worker ! ForegroundTask(path, System.currentTimeMillis() + 10000, taskStr1)
        expectMsgType[BackgroundTask]
        expectMsgType[ShardTaskComplete]

        val taskStr2 = StringSerializer.serializeToString(HyperStorageContentPatch(path,
          DynamicBody(ObjV("text" → "abc", "text2" → "klmn"))
        ))
        worker ! ForegroundTask(path, System.currentTimeMillis() + 10000, taskStr2)
        val backgroundWorkerTask = expectMsgType[BackgroundTask]
        expectMsgType[ShardTaskComplete]

        val transactionUuids = whenReady(db.selectContent(path, "")) { result =>
          result.get.transactionList
        }

        selectTransactions(transactionUuids, path, db) foreach {
          _.completedAt shouldBe None
        }

        val backgroundWorker = TestActorRef(BackgroundWorker.props(hyperbus, db, tracker, self))

        FaultClientTransport.checkers += {
          case request: DynamicRequest ⇒
            if (request.method == Method.FEED_PUT) {
              true
            } else {
              fail("Unexpected publish")
              false
            }
        }

        backgroundWorker ! backgroundWorkerTask
        expectMsgType[ShardTaskComplete].result shouldBe a[BackgroundTaskFailedException]
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

        backgroundWorker ! backgroundWorkerTask
        expectMsgType[ShardTaskComplete].result shouldBe a[BackgroundTaskFailedException]
        val mons = selectTransactions(transactionUuids, path, db)

        mons.head.completedAt shouldBe None
        mons.tail.head.completedAt shouldNot be(None)

        FaultClientTransport.checkers.clear()
        backgroundWorker ! backgroundWorkerTask
        expectMsgType[ShardTaskComplete].result shouldBe a[BackgroundTaskResult]
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

        val worker = TestActorRef(ForegroundWorker.props(hyperbus, db, tracker, 10.seconds))

        val task = HyperStorageContentPut(
          path = "collection-1~/test-resource-1",
          DynamicBody(ObjV("text" → "Test item value", "null" → Null))
        )

        db.selectContent("collection-1~", "test-resource-1").futureValue shouldBe None

        val taskStr = StringSerializer.serializeToString(task)
        worker ! ForegroundTask("collection-1~", System.currentTimeMillis() + 10000, taskStr)
        val backgroundWorkerTask = expectMsgType[BackgroundTask]
        backgroundWorkerTask.documentUri should equal("collection-1~")
        val workerResult = expectMsgType[ShardTaskComplete]
        val r = response(workerResult.result.asInstanceOf[ForegroundWorkerTaskResult].content)
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
        worker ! ForegroundTask("collection-1~", System.currentTimeMillis() + 10000, task2Str)
        val backgroundWorkerTask2 = expectMsgType[BackgroundTask]
        backgroundWorkerTask2.documentUri should equal("collection-1~")
        val workerResult2 = expectMsgType[ShardTaskComplete]
        val r2 = response(workerResult2.result.asInstanceOf[ForegroundWorkerTaskResult].content)
        r2.statusCode should equal(Status.CREATED)
        r2.correlationId should equal(task2.correlationId)

        val content2 = db.selectContent("collection-1~", "test-resource-2").futureValue
        content2 shouldNot equal(None)
        content2.get.body should equal(Some("""{"text":"Test item value 2","id":"test-resource-2"}"""))
        content2.get.transactionList.size should equal(2)
        content2.get.revision should equal(2)

        val transactions = selectTransactions(content2.get.transactionList, "collection-1~", db)
        transactions.size should equal(2)
        transactions.foreach {
          _.completedAt shouldBe None
        }
        transactions.head.revision should equal(2)
        transactions.tail.head.revision should equal(1)

        val backgroundWorker = TestActorRef(BackgroundWorker.props(hyperbus, db, tracker, self))
        backgroundWorker ! backgroundWorkerTask
        val backgroundWorkerResult = expectMsgType[ShardTaskComplete]
        val rc = backgroundWorkerResult.result.asInstanceOf[BackgroundTaskResult]
        rc.documentUri should equal("collection-1~")
        rc.transactions should equal(content2.get.transactionList.reverse)

        eventually {
          db.selectContentStatic("collection-1~").futureValue.get.transactionList shouldBe empty
        }
        selectTransactions(content2.get.transactionList, "collection-1~", db).foreach {
          _.completedAt shouldNot be(None)
        }
      }

      "Patch item" in {
        val hyperbus = testHyperbus()
        val tk = testKit()
        import tk._

        val worker = TestActorRef(ForegroundWorker.props(hyperbus, db, tracker, 10.seconds))

        val path = "collection-1~/test-resource-" + UUID.randomUUID().toString
        val ResourcePath(documentUri, itemSegment) = ContentLogic.splitPath(path)

        val taskPutStr = StringSerializer.serializeToString(HyperStorageContentPut(path,
          DynamicBody(ObjV("text1" → "abc", "text2" → "klmn"))
        ))

        worker ! ForegroundTask(documentUri, System.currentTimeMillis() + 10000, taskPutStr)
        expectMsgType[BackgroundTask]
        expectMsgType[ShardTaskComplete]

        val task = HyperStorageContentPatch(path,
          DynamicBody(ObjV("text1" → "efg", "text2" → Null, "text3" → "zzz"))
        )
        val taskPatchStr = StringSerializer.serializeToString(task)
        worker ! ForegroundTask(documentUri, System.currentTimeMillis() + 10000, taskPatchStr)

        expectMsgType[BackgroundTask]
        expectMsgPF() {
          case ShardTaskComplete(_, result: ForegroundWorkerTaskResult) if response(result.content).statusCode == Status.OK &&
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
        worker ! ForegroundTask(documentUri, System.currentTimeMillis() + 10000, deleteTaskStr)
        expectMsgType[BackgroundTask]
        expectMsgType[ShardTaskComplete]

        // now patch should return 404
        worker ! ForegroundTask(documentUri, System.currentTimeMillis() + 10000, taskPatchStr)

        expectMsgPF() {
          case ShardTaskComplete(_, result: ForegroundWorkerTaskResult) if response(result.content).statusCode == Status.NOT_FOUND &&
            response(result.content).correlationId == task.correlationId ⇒ {
            true
          }
        }
      }

      "Delete item" in {
        val hyperbus = testHyperbus()
        val tk = testKit()
        import tk._

        val worker = TestActorRef(ForegroundWorker.props(hyperbus, db, tracker, 10.seconds))

        val path = "collection-1~/test-resource-" + UUID.randomUUID().toString
        val ResourcePath(documentUri, itemSegment) = ContentLogic.splitPath(path)

        val taskPutStr = StringSerializer.serializeToString(HyperStorageContentPut(path,
          DynamicBody(ObjV("text1" → "abc", "text2" → "klmn"))
        ))

        worker ! ForegroundTask(documentUri, System.currentTimeMillis() + 10000, taskPutStr)
        expectMsgType[BackgroundTask]
        expectMsgType[ShardTaskComplete]

        val task = HyperStorageContentDelete(path)
        val taskStr = StringSerializer.serializeToString(task)
        worker ! ForegroundTask(documentUri, System.currentTimeMillis() + 10000, taskStr)

        expectMsgType[BackgroundTask]
        expectMsgPF() {
          case ShardTaskComplete(_, result: ForegroundWorkerTaskResult) if response(result.content).statusCode == Status.OK &&
            response(result.content).correlationId == task.correlationId ⇒ {
            true
          }
        }

        db.selectContent(documentUri, itemSegment).futureValue shouldBe None
      }

      "Delete collection" in {
        val hyperbus = testHyperbus()
        val tk = testKit()
        import tk._

        val worker = TestActorRef(ForegroundWorker.props(hyperbus, db, tracker, 10.seconds))

        val path = UUID.randomUUID().toString + "~/el1"
        val ResourcePath(documentUri, itemSegment) = ContentLogic.splitPath(path)

        val taskPutStr = StringSerializer.serializeToString(HyperStorageContentPut(path,
          DynamicBody(ObjV("text1" → "abc", "text2" → "klmn"))
        ))

        worker ! ForegroundTask(documentUri, System.currentTimeMillis() + 10000, taskPutStr)
        expectMsgType[BackgroundTask]
        expectMsgType[ShardTaskComplete]

        val task = HyperStorageContentDelete(documentUri)
        val taskStr = StringSerializer.serializeToString(task)
        worker ! ForegroundTask(documentUri, System.currentTimeMillis() + 10000, taskStr)

        expectMsgType[BackgroundTask]
        expectMsgPF() {
          case ShardTaskComplete(_, result: ForegroundWorkerTaskResult) if response(result.content).statusCode == Status.OK &&
            response(result.content).correlationId == task.correlationId ⇒ {
            true
          }
        }

        db.selectContent(documentUri, itemSegment).futureValue.get.isDeleted shouldBe true
        db.selectContent(documentUri, "").futureValue.get.isDeleted shouldBe true
      }
    }

    "HyperStorage integrated" - {
      "Test hyper-storage PUT+GET+Event" in {
        val hyperbus = testHyperbus()
        val tk = testKit()
        import tk._
        import system._

        cleanUpCassandra()

        val workerProps = ForegroundWorker.props(hyperbus, db, tracker, 10.seconds)
        val backgroundWorkerProps = BackgroundWorker.props(hyperbus, db, tracker, self)
        val workerSettings = Map(
          "hyper-storage-foreground-worker" → (workerProps, 1, "fgw-"),
          "hyper-storage-background-worker" → (backgroundWorkerProps, 1, "bgw-")
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

        val workerProps = ForegroundWorker.props(hyperbus, db, tracker, 10.seconds)
        val backgroundWorkerProps = BackgroundWorker.props(hyperbus, db, tracker, self)
        val workerSettings = Map(
          "hyper-storage-foreground-worker" → (workerProps, 1, "fgw-"),
          "hyper-storage-background-worker" → (backgroundWorkerProps, 1, "bgw-")
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

        val workerProps = ForegroundWorker.props(hyperbus, db, tracker, 10.seconds)
        val backgroundWorkerProps = BackgroundWorker.props(hyperbus, db, tracker, self)
        val workerSettings = Map(
          "hyper-storage-foreground-worker" → (workerProps, 1, "fgw-"),
          "hyper-storage-background-worker" → (backgroundWorkerProps, 1, "bgw-")
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
          body = new QueryBuilder() add("size", 50) result())

        whenReady(f4) { response ⇒
          response.statusCode should equal(Status.OK)
          response.body.content should equal(
            ObjV("_embedded" -> ObjV("els" → LstV(c1x, c2x)))
          )
        }

        import Sort._

        val f5 = hyperbus <~ HyperStorageContentGet("collection-1~",
          body = new QueryBuilder() add("size", 50) sortBy (Seq(SortBy("id", true))) result())

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

        val workerProps = ForegroundWorker.props(hyperbus, db, tracker, 10.seconds)
        val backgroundWorkerProps = BackgroundWorker.props(hyperbus, db, tracker, self)
        val workerSettings = Map(
          "hyper-storage-foreground-worker" → (workerProps, 1, "fgw-"),
          "hyper-storage-background-worker" → (backgroundWorkerProps, 1, "bgw-")
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
          body = new QueryBuilder() add("size", 50) result())

        whenReady(f4) { response ⇒
          response.statusCode should equal(Status.OK)
          response.body.content should equal(
            ObjV("_embedded" -> ObjV("els" → LstV(c1x, c2x)))
          )
        }

        import Sort._

        val f5 = hyperbus <~ HyperStorageContentGet("collection-2~",
          body = new QueryBuilder() add("size", 50) sortBy (Seq(SortBy("id", true))) result())
        whenReady(f5) { response ⇒
          response.statusCode should equal(Status.OK)
          response.body.content should equal(
            ObjV("_embedded" -> ObjV("els" → LstV(c2x, c1x)))
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

        val worker = TestActorRef(ForegroundWorker.props(hyperbus, db, tracker, 10.seconds))
        val path = "incomplete-" + UUID.randomUUID().toString
        val taskStr1 = StringSerializer.serializeToString(HyperStorageContentPut(path,
          DynamicBody(ObjV("text" → "Test resource value", "null" → Null))
        ))
        worker ! ForegroundTask(path, System.currentTimeMillis() + 10000, taskStr1)
        val backgroundWorkerTask = expectMsgType[BackgroundTask]
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

        val backgroundWorkerTask2 = processorProbe.expectMsgType[BackgroundTask](max = 30.seconds)
        backgroundWorkerTask.documentUri should equal(backgroundWorkerTask2.documentUri)
        processorProbe.reply(BackgroundTaskFailedException(backgroundWorkerTask2.documentUri, "Testing worker behavior"))
        val backgroundWorkerTask3 = processorProbe.expectMsgType[BackgroundTask](max = 30.seconds)
        hotWorker ! processorProbe.reply(BackgroundTaskResult(backgroundWorkerTask2.documentUri, transactionUuids))
        gracefulStop(hotWorker, 30 seconds, ShutdownRecoveryWorker).futureValue(TestTimeout(30.seconds))
      }

      "StaleRecoveryWorker" in {
        val hyperbus = testHyperbus()
        val tk = testKit()
        import tk._

        cleanUpCassandra()

        val worker = TestActorRef(ForegroundWorker.props(hyperbus, db, tracker, 10.seconds))
        val path = "incomplete-" + UUID.randomUUID().toString
        val taskStr1 = StringSerializer.serializeToString(HyperStorageContentPut(path,
          DynamicBody(ObjV("text" → "Test resource value", "null" → Null))
        ))
        val millis = System.currentTimeMillis()
        worker ! ForegroundTask(path, System.currentTimeMillis() + 10000, taskStr1)
        val backgroundWorkerTask = expectMsgType[BackgroundTask]
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

        val backgroundWorkerTask2 = processorProbe.expectMsgType[BackgroundTask](max = 30.seconds)
        backgroundWorkerTask.documentUri should equal(backgroundWorkerTask2.documentUri)
        processorProbe.reply(BackgroundTaskFailedException(backgroundWorkerTask2.documentUri, "Testing worker behavior"))

        eventually {
          db.selectCheckpoint(transaction.partition).futureValue shouldBe Some(newTransaction.dtQuantum - 1)
        }

        val backgroundWorkerTask3 = processorProbe.expectMsgType[BackgroundTask](max = 30.seconds)
        hotWorker ! processorProbe.reply(BackgroundTaskResult(backgroundWorkerTask2.documentUri, newContent.transactionList))

        eventually {
          db.selectCheckpoint(transaction.partition).futureValue.get shouldBe >(newTransaction.dtQuantum)
        }

        val backgroundWorkerTask4 = processorProbe.expectMsgType[BackgroundTask](max = 30.seconds) // this is abandoned
        hotWorker ! processorProbe.reply(BackgroundTaskResult(backgroundWorkerTask4.documentUri, List()))

        gracefulStop(hotWorker, 30 seconds, ShutdownRecoveryWorker).futureValue(TestTimeout(30.seconds))
      }
    }

    "Indexing" - {
      "Create index without sorting or filtering" in {
        cleanUpCassandra()
        val hyperbus = integratedHyperbus(db)

        val c1 = ObjV("a" → "hello", "b" → 100500)
        val f1 = hyperbus <~ HyperStorageContentPut("collection-1~/item1", DynamicBody(c1))
        f1.futureValue.statusCode should equal(Status.CREATED)

        val path = "collection-1~"
        val f2 = hyperbus <~ HyperStorageIndexPost(path, HyperStorageIndexNew(Some("index1"), Seq.empty, None))
        f2.futureValue.statusCode should equal(Status.CREATED)

        val indexDef = db.selectIndexDef("collection-1~", "index1").futureValue
        indexDef shouldBe defined
        indexDef.get.documentUri shouldBe "collection-1~"
        indexDef.get.indexId shouldBe "index1"

        eventually {
          val indexDefUp = db.selectIndexDef("collection-1~", "index1").futureValue
          indexDefUp shouldBe defined
          indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
        }

        eventually {
          val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
          indexContent.size shouldBe 1
          indexContent.head.documentUri shouldBe "collection-1~"
          indexContent.head.itemSegment shouldBe "item1"
          indexContent.head.body.get should include("\"item1\"")
        }

        val c3 = ObjV("a" → "goodbye", "b" → 123456)
        val f3 = hyperbus <~ HyperStorageContentPut("collection-1~/item2", DynamicBody(c3))
        f3.futureValue.statusCode should equal(Status.CREATED)

        eventually {
          val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
          indexContent.size shouldBe 2
          indexContent(1).documentUri shouldBe "collection-1~"
          indexContent(1).itemSegment shouldBe "item2"
          indexContent(1).body.get should include("\"item2\"")
        }
      }

      "Create index with filter" in {
        cleanUpCassandra()
        val hyperbus = integratedHyperbus(db)

        val c1 = ObjV("a" → "hello", "b" → 100500)
        val f1 = hyperbus <~ HyperStorageContentPut("collection-1~/item1", DynamicBody(c1))
        f1.futureValue.statusCode should equal(Status.CREATED)

        val path = "collection-1~"
        val fi = hyperbus <~ HyperStorageIndexPost(path, HyperStorageIndexNew(Some("index1"), Seq.empty, Some("b > 10")))
        fi.futureValue.statusCode should equal(Status.CREATED)

        val indexDef = db.selectIndexDef("collection-1~", "index1").futureValue
        indexDef shouldBe defined
        indexDef.get.documentUri shouldBe "collection-1~"
        indexDef.get.indexId shouldBe "index1"

        eventually {
          val indexDefUp = db.selectIndexDef("collection-1~", "index1").futureValue
          indexDefUp shouldBe defined
          indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
        }

        eventually {
          val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
          indexContent.size shouldBe 1
          indexContent.head.documentUri shouldBe "collection-1~"
          indexContent.head.itemSegment shouldBe "item1"
          indexContent.head.body.get should include("\"item1\"")
        }

        val c2 = ObjV("a" → "goodbye", "b" → 1)
        val f2 = hyperbus <~ HyperStorageContentPut("collection-1~/item2", DynamicBody(c2))
        f2.futureValue.statusCode should equal(Status.CREATED)

        val c3 = ObjV("a" → "way way", "b" → 12)
        val f3 = hyperbus <~ HyperStorageContentPut("collection-1~/item3", DynamicBody(c3))
        f3.futureValue.statusCode should equal(Status.CREATED)

        eventually {
          val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
          indexContent.size shouldBe 2
          indexContent(1).documentUri shouldBe "collection-1~"
          indexContent(1).itemSegment shouldBe "item3"
          indexContent(1).body.get should include("\"item3\"")
        }
      }

      "Create index with filter and decimal sorting" in {
        cleanUpCassandra()
        val hyperbus = integratedHyperbus(db)

        val c1 = ObjV("a" → "hello", "b" → 100500)
        val f1 = hyperbus <~ HyperStorageContentPut("collection-1~/item1", DynamicBody(c1))
        f1.futureValue.statusCode should equal(Status.CREATED)

        val path = "collection-1~"
        val fi = hyperbus <~ HyperStorageIndexPost(path, HyperStorageIndexNew(Some("index1"),
          Seq(HyperStorageIndexSortItem("b", order = Some("asc"), fieldType = Some("decimal"))), Some("b > 10")))
        fi.futureValue.statusCode should equal(Status.CREATED)

        val indexDef = db.selectIndexDef("collection-1~", "index1").futureValue
        indexDef shouldBe defined
        indexDef.get.documentUri shouldBe "collection-1~"
        indexDef.get.indexId shouldBe "index1"

        eventually {
          val indexDefUp = db.selectIndexDef("collection-1~", "index1").futureValue
          indexDefUp shouldBe defined
          indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
        }

        eventually {
          val indexContent = db.selectIndexCollection("index_content_da0", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
          indexContent.size shouldBe 1
          indexContent.head.documentUri shouldBe "collection-1~"
          indexContent.head.itemSegment shouldBe "item1"
          indexContent.head.body.get should include("\"item1\"")
        }

        val c2 = ObjV("a" → "goodbye", "b" → 1)
        val f2 = hyperbus <~ HyperStorageContentPut("collection-1~/item2", DynamicBody(c2))
        f2.futureValue.statusCode should equal(Status.CREATED)

        val c3 = ObjV("a" → "way way", "b" → 12)
        val f3 = hyperbus <~ HyperStorageContentPut("collection-1~/item3", DynamicBody(c3))
        f3.futureValue.statusCode should equal(Status.CREATED)

        eventually {
          val indexContent = db.selectIndexCollection("index_content_da0", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
          indexContent.size shouldBe 2
          indexContent(0).documentUri shouldBe "collection-1~"
          indexContent(0).itemSegment shouldBe "item3"
          indexContent(0).body.get should include("\"item3\"")

          indexContent(1).documentUri shouldBe "collection-1~"
          indexContent(1).itemSegment shouldBe "item1"
          indexContent(1).body.get should include("\"item1\"")
        }
      }

      "Create index with filter and decimal desc sorting" in {
        cleanUpCassandra()
        val hyperbus = integratedHyperbus(db)

        val c1 = ObjV("a" → "hello", "b" → 100500)
        val f1 = hyperbus <~ HyperStorageContentPut("collection-1~/item1", DynamicBody(c1))
        f1.futureValue.statusCode should equal(Status.CREATED)

        val path = "collection-1~"
        val fi = hyperbus <~ HyperStorageIndexPost(path, HyperStorageIndexNew(Some("index1"),
          Seq(HyperStorageIndexSortItem("b", order = Some("desc"), fieldType = Some("decimal"))), Some("b > 10")))
        fi.futureValue.statusCode should equal(Status.CREATED)

        val indexDef = db.selectIndexDef("collection-1~", "index1").futureValue
        indexDef shouldBe defined
        indexDef.get.documentUri shouldBe "collection-1~"
        indexDef.get.indexId shouldBe "index1"

        eventually {
          val indexDefUp = db.selectIndexDef("collection-1~", "index1").futureValue
          indexDefUp shouldBe defined
          indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
        }

        eventually {
          val indexContent = db.selectIndexCollection("index_content_dd0", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
          indexContent.size shouldBe 1
          indexContent.head.documentUri shouldBe "collection-1~"
          indexContent.head.itemSegment shouldBe "item1"
          indexContent.head.body.get should include("\"item1\"")
        }

        val c2 = ObjV("a" → "goodbye", "b" → 1)
        val f2 = hyperbus <~ HyperStorageContentPut("collection-1~/item2", DynamicBody(c2))
        f2.futureValue.statusCode should equal(Status.CREATED)

        val c3 = ObjV("a" → "way way", "b" → 12)
        val f3 = hyperbus <~ HyperStorageContentPut("collection-1~/item3", DynamicBody(c3))
        f3.futureValue.statusCode should equal(Status.CREATED)

        eventually {
          val indexContent = db.selectIndexCollection("index_content_dd0", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
          indexContent.size shouldBe 2
          indexContent(0).documentUri shouldBe "collection-1~"
          indexContent(0).itemSegment shouldBe "item1"
          indexContent(0).body.get should include("\"item1\"")

          indexContent(1).documentUri shouldBe "collection-1~"
          indexContent(1).itemSegment shouldBe "item3"
          indexContent(1).body.get should include("\"item3\"")
        }
      }

      "Create index with filter and text sorting" in {
        cleanUpCassandra()
        val hyperbus = integratedHyperbus(db)

        val c1 = ObjV("a" → "hello", "b" → 100500)
        val f1 = hyperbus <~ HyperStorageContentPut("collection-1~/item1", DynamicBody(c1))
        f1.futureValue.statusCode should equal(Status.CREATED)

        val path = "collection-1~"
        val fi = hyperbus <~ HyperStorageIndexPost(path, HyperStorageIndexNew(Some("index1"),
          Seq(HyperStorageIndexSortItem("a", order = Some("asc"), fieldType = Some("text"))), Some("b > 10")))
        fi.futureValue.statusCode should equal(Status.CREATED)

        val indexDef = db.selectIndexDef("collection-1~", "index1").futureValue
        indexDef shouldBe defined
        indexDef.get.documentUri shouldBe "collection-1~"
        indexDef.get.indexId shouldBe "index1"

        eventually {
          val indexDefUp = db.selectIndexDef("collection-1~", "index1").futureValue
          indexDefUp shouldBe defined
          indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
        }

        eventually {
          val indexContent = db.selectIndexCollection("index_content_ta0", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
          indexContent.size shouldBe 1
          indexContent.head.documentUri shouldBe "collection-1~"
          indexContent.head.itemSegment shouldBe "item1"
          indexContent.head.body.get should include("\"item1\"")
        }

        val c2 = ObjV("a" → "goodbye", "b" → 1)
        val f2 = hyperbus <~ HyperStorageContentPut("collection-1~/item2", DynamicBody(c2))
        f2.futureValue.statusCode should equal(Status.CREATED)

        val c3 = ObjV("a" → "way way", "b" → 12)
        val f3 = hyperbus <~ HyperStorageContentPut("collection-1~/item3", DynamicBody(c3))
        f3.futureValue.statusCode should equal(Status.CREATED)

        eventually {
          val indexContent = db.selectIndexCollection("index_content_ta0", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
          indexContent.size shouldBe 2
          indexContent(0).documentUri shouldBe "collection-1~"
          indexContent(0).itemSegment shouldBe "item1"
          indexContent(0).body.get should include("\"item1\"")

          indexContent(1).documentUri shouldBe "collection-1~"
          indexContent(1).itemSegment shouldBe "item3"
          indexContent(1).body.get should include("\"item3\"")
        }
      }

      "Create index with filter and text desc sorting" in {
        cleanUpCassandra()
        val hyperbus = integratedHyperbus(db)

        val c1 = ObjV("a" → "hello", "b" → 100500)
        val f1 = hyperbus <~ HyperStorageContentPut("collection-1~/item1", DynamicBody(c1))
        f1.futureValue.statusCode should equal(Status.CREATED)

        val path = "collection-1~"
        val fi = hyperbus <~ HyperStorageIndexPost(path, HyperStorageIndexNew(Some("index1"),
          Seq(HyperStorageIndexSortItem("a", order = Some("desc"), fieldType = Some("text"))), Some("b > 10")))
        fi.futureValue.statusCode should equal(Status.CREATED)

        val indexDef = db.selectIndexDef("collection-1~", "index1").futureValue
        indexDef shouldBe defined
        indexDef.get.documentUri shouldBe "collection-1~"
        indexDef.get.indexId shouldBe "index1"

        eventually {
          val indexDefUp = db.selectIndexDef("collection-1~", "index1").futureValue
          indexDefUp shouldBe defined
          indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
        }

        eventually {
          val indexContent = db.selectIndexCollection("index_content_td0", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
          indexContent.size shouldBe 1
          indexContent.head.documentUri shouldBe "collection-1~"
          indexContent.head.itemSegment shouldBe "item1"
          indexContent.head.body.get should include("\"item1\"")
        }

        val c2 = ObjV("a" → "goodbye", "b" → 1)
        val f2 = hyperbus <~ HyperStorageContentPut("collection-1~/item2", DynamicBody(c2))
        f2.futureValue.statusCode should equal(Status.CREATED)

        val c3 = ObjV("a" → "way way", "b" → 12)
        val f3 = hyperbus <~ HyperStorageContentPut("collection-1~/item3", DynamicBody(c3))
        f3.futureValue.statusCode should equal(Status.CREATED)

        eventually {
          val indexContent = db.selectIndexCollection("index_content_td0", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
          indexContent.size shouldBe 2
          indexContent(0).documentUri shouldBe "collection-1~"
          indexContent(0).itemSegment shouldBe "item3"
          indexContent(0).body.get should include("\"item3\"")

          indexContent(1).documentUri shouldBe "collection-1~"
          indexContent(1).itemSegment shouldBe "item1"
          indexContent(1).body.get should include("\"item1\"")
        }
      }

      "Create and delete index" in {
        cleanUpCassandra()
        val hyperbus = integratedHyperbus(db)

        val path = "collection-1~"
        val f2 = hyperbus <~ HyperStorageIndexPost(path, HyperStorageIndexNew(Some("index1"), Seq.empty, None))
        f2.futureValue.statusCode should equal(Status.CREATED)

        val c1 = ObjV("a" → "hello", "b" → 100500)
        val f1 = hyperbus <~ HyperStorageContentPut("collection-1~/item1", DynamicBody(c1))
        f1.futureValue.statusCode should equal(Status.CREATED)

        val indexDef = db.selectIndexDef("collection-1~", "index1").futureValue
        indexDef shouldBe defined
        indexDef.get.documentUri shouldBe "collection-1~"
        indexDef.get.indexId shouldBe "index1"

        eventually {
          val indexDefUp = db.selectIndexDef("collection-1~", "index1").futureValue
          indexDefUp shouldBe defined
          indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
          val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
          indexContent.size shouldBe 1
          indexContent.head.documentUri shouldBe "collection-1~"
          indexContent.head.itemSegment shouldBe "item1"
          indexContent.head.body.get should include("\"item1\"")
        }

        val f3 = hyperbus <~ HyperStorageIndexDelete(path, "index1")
        f3.futureValue.statusCode should equal(Status.NO_CONTENT)

        eventually {
          val indexDefUp = db.selectIndexDef("collection-1~", "index1").futureValue
          indexDefUp shouldBe None
          val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
          indexContent shouldBe empty
        }
      }

      "Collection removal should remove index" in {
        cleanUpCassandra()
        val hyperbus = integratedHyperbus(db)

        val path = "collection-1~"
        val f2 = hyperbus <~ HyperStorageIndexPost(path, HyperStorageIndexNew(Some("index1"), Seq.empty, None))
        f2.futureValue.statusCode should equal(Status.CREATED)

        val c1 = ObjV("a" → "hello", "b" → 100500)
        val f1 = hyperbus <~ HyperStorageContentPut("collection-1~/item1", DynamicBody(c1))
        f1.futureValue.statusCode should equal(Status.CREATED)

        val indexDef = db.selectIndexDef("collection-1~", "index1").futureValue
        indexDef shouldBe defined
        indexDef.get.documentUri shouldBe "collection-1~"
        indexDef.get.indexId shouldBe "index1"

        eventually {
          val indexDefUp = db.selectIndexDef("collection-1~", "index1").futureValue
          indexDefUp shouldBe defined
          indexDefUp.get.status shouldBe IndexDef.STATUS_NORMAL
          val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
          indexContent.size shouldBe 1
          indexContent.head.documentUri shouldBe "collection-1~"
          indexContent.head.itemSegment shouldBe "item1"
          indexContent.head.body.get should include("\"item1\"")
        }

        val f3 = hyperbus <~ HyperStorageContentDelete(path)
        f3.futureValue.statusCode should equal(Status.OK)

        eventually {
          val indexDefUp = db.selectIndexDef("collection-1~", "index1").futureValue
          indexDefUp shouldBe None
          val indexContent = db.selectIndexCollection("index_content", "collection-1~", "index1", Seq.empty, Seq.empty, 10).futureValue.toSeq
          indexContent shouldBe empty
        }
      }
    }
  }

  // todo: StringDeserializer should return DynamicBody here
  def response(content: String): Response[DynamicBody] = StringDeserializer.dynamicResponse(content).asInstanceOf[Response[DynamicBody]]
}
