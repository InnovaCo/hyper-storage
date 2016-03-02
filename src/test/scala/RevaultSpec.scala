import java.util.UUID

import akka.actor.{ActorSelection, Address, Props}
import akka.testkit.{TestActorRef, TestProbe}
import akka.util.Timeout
import com.datastax.driver.core.utils.UUIDs
import eu.inn.binders.dynamic.{Null, Obj, Text}
import eu.inn.hyperbus.model._
import eu.inn.hyperbus.serialization.{StringDeserializer, StringSerializer}
import eu.inn.revault._
import eu.inn.revault.db.Content
import eu.inn.revault.protocol._
import eu.inn.revault.recovery.{StaleRecoveryWorker, HotRecoveryWorker, ShutdownRecoveryWorker}
import eu.inn.revault.sharding.ShardMemberStatus.Active
import eu.inn.revault.sharding._
import mock.FaultClientTransport
import org.scalatest.concurrent.PatienceConfiguration.{Timeout ⇒ TestTimeout}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FreeSpec, Matchers}
import akka.pattern.gracefulStop

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

// todo: split revault, shardprocessor and single nodes test
class RevaultSpec extends FreeSpec
  with Matchers
  with ScalaFutures
  with CassandraFixture
  with TestHelpers
  with Eventually {

  import ContentLogic._
  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(2000, Millis)))

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

    "Revault worker" - {
      "Put" in {
        val hyperBus = testHyperBus()
        val tk = testKit()
        import tk._

        cleanUpCassandra()

        val worker = TestActorRef(new RevaultWorker(hyperBus, db, 10.seconds))

        val task = RevaultPut(
          path = "test-resource-1",
          DynamicBody(Obj(Map("text" → Text("Test resource value"), "null" → Null)))
        )

        whenReady(db.selectContent("test-resource-1", "")) { result =>
          result shouldBe None
        }

        val taskStr = StringSerializer.serializeToString(task)
        worker ! RevaultTask("", System.currentTimeMillis() + 10000, taskStr)
        val completerTask = expectMsgType[RevaultCompleterTask]
        completerTask.documentUri should equal(task.path)
        val workerResult = expectMsgType[ShardTaskComplete]
        val r = response(workerResult.result.asInstanceOf[RevaultTaskResult].content)
        r.status should equal(Status.CREATED)
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

        val completer = TestActorRef(new RevaultCompleter(hyperBus, db))
        completer ! completerTask
        val completerResult = expectMsgType[ShardTaskComplete]
        val rc = completerResult.result.asInstanceOf[RevaultCompleterTaskResult]
        rc.documentUri should equal("test-resource-1")
        rc.transactions should contain(uuid)
        selectTransactions(rc.transactions, "test-resource-1", db) foreach { transaction ⇒
          transaction.completedAt shouldNot be(None)
          transaction.revision should equal(1)
        }
        db.selectContent("test-resource-1", "").futureValue.get.transactionList shouldBe empty
      }

      "Patch resource that doesn't exists" in {
        val hyperBus = testHyperBus()
        val tk = testKit()
        import tk._

        cleanUpCassandra()

        val worker = TestActorRef(new RevaultWorker(hyperBus, db, 10.seconds))

        val task = RevaultPatch(
          path = "not-existing",
          DynamicBody(Obj(Map("text" → Text("Test resource value"))))
        )

        val taskStr = StringSerializer.serializeToString(task)
        worker ! RevaultTask("", System.currentTimeMillis() + 10000, taskStr)
        expectMsgPF() {
          case ShardTaskComplete(_, result: RevaultTaskResult) if response(result.content).status == Status.NOT_FOUND &&
            response(result.content).correlationId == task.correlationId ⇒ {
            true
          }
        }

        whenReady(db.selectContent("not-existing", "")) { result =>
          result shouldBe None
        }
      }

      "Patch existing resource" in {
        val hyperBus = testHyperBus()
        val tk = testKit()
        import tk._

        val worker = TestActorRef(new RevaultWorker(hyperBus, db, 10.seconds))

        val path = "test-resource-" + UUID.randomUUID().toString
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
          case ShardTaskComplete(_, result: RevaultTaskResult) if response(result.content).status == Status.OK &&
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

        cleanUpCassandra()

        val worker = TestActorRef(new RevaultWorker(hyperBus, db, 10.seconds))

        val task = RevaultDelete(path = "not-existing", body = EmptyBody)

        val taskStr = StringSerializer.serializeToString(task)
        worker ! RevaultTask("", System.currentTimeMillis() + 10000, taskStr)
        expectMsgPF() {
          case ShardTaskComplete(_, result: RevaultTaskResult) if response(result.content).status == Status.NOT_FOUND &&
            response(result.content).correlationId == task.correlationId ⇒ {
            true
          }
        }

        whenReady(db.selectContent("not-existing", "")) { result =>
          result shouldBe None
        }
      }

      "Delete resource that exists" in {
        val hyperBus = testHyperBus()
        val tk = testKit()
        import tk._

        cleanUpCassandra()

        val worker = TestActorRef(new RevaultWorker(hyperBus, db, 10.seconds))

        val path = "test-resource-" + UUID.randomUUID().toString
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
          case ShardTaskComplete(_, result: RevaultTaskResult) if response(result.content).status == Status.OK &&
            response(result.content).correlationId == task.correlationId ⇒ {
            true
          }
        }

        whenReady(db.selectContent(path, "")) { result =>
          result.get.isDeleted shouldBe true
        }
      }

      "Test multiple transactions" in {
        val hyperBus = testHyperBus()
        val tk = testKit()
        import tk._

        cleanUpCassandra()

        val worker = TestActorRef(new RevaultWorker(hyperBus, db, 10.seconds))
        val path = "abcde"
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

        val taskStr3 = StringSerializer.serializeToString(RevaultDelete(path, EmptyBody))
        worker ! RevaultTask("", System.currentTimeMillis() + 10000, taskStr3)
        val completerTask = expectMsgType[RevaultCompleterTask]
        val workerResult = expectMsgType[ShardTaskComplete]
        val r = response(workerResult.result.asInstanceOf[RevaultTaskResult].content)
        r.status should equal(Status.OK)

        val transactions = whenReady(db.selectContent(path, "")) { result =>
          result.get.isDeleted should equal(true)
          result.get.transactionList
        }

        selectTransactions(transactions, path, db) foreach { transaction ⇒
          transaction.completedAt should be(None)
        }

        val completer = TestActorRef(new RevaultCompleter(hyperBus, db))
        completer ! completerTask
        val completerResult = expectMsgType[ShardTaskComplete]
        val rc = completerResult.result.asInstanceOf[RevaultCompleterTaskResult]
        rc.documentUri should equal(path)
        rc.transactions should equal(transactions.reverse)

        selectTransactions(rc.transactions, path, db) foreach { transaction ⇒
          transaction.completedAt shouldNot be(None)
        }
      }

      "Test faulty publish" in {
        val hyperBus = testHyperBus()
        val tk = testKit()
        import tk._

        cleanUpCassandra()

        val worker = TestActorRef(new RevaultWorker(hyperBus, db, 10.seconds))
        val path = "faulty"
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

        val transactionUuids = whenReady(db.selectContent(path, "")) { result =>
          result.get.transactionList
        }

        selectTransactions(transactionUuids, path, db) foreach {
          _.completedAt shouldBe None
        }

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
        expectMsgType[ShardTaskComplete].result shouldBe a[RevaultCompleterTaskResult]
        selectTransactions(transactionUuids, path, db) foreach {
          _.completedAt shouldNot be(None)
        }

        db.selectContent(path, "").futureValue.get.transactionList shouldBe empty
      }
    }

    "Revault worker (collections)" - {
      /*"Put item" in {
        val hyperBus = testHyperBus()
        val tk = testKit()
        import tk._

        cleanUpCassandra()

        val worker = TestActorRef(new RevaultWorker(hyperBus, db, 10.seconds))

        val task = RevaultPut(
          path = "collection-1/test-resource-1",
          DynamicBody(Obj(Map("text" → Text("Test item value"), "null" → Null)))
        )

        db.selectContent("collection-1", "test-resource-1").futureValue shouldBe None

        val taskStr = StringSerializer.serializeToString(task)
        worker ! RevaultTask("", System.currentTimeMillis() + 10000, taskStr)
        val completerTask = expectMsgType[RevaultCompleterTask]
        completerTask.documentUri should equal("collection-1")
        val workerResult = expectMsgType[ShardTaskComplete]
        val r = response(workerResult.result.asInstanceOf[RevaultTaskResult].content)
        r.status should equal(Status.CREATED)
        r.correlationId should equal(task.correlationId)

        val content = db.selectContent("collection-1", "test-resource-1").futureValue
        content shouldNot equal(None)
        content.get.body should equal(Some("""{"text":"Test item value"}"""))
        content.get.transactionList.size should equal(1)
        val uuid = content.get.transactionList.head
/*
        selectTransactions(content.get.transactionList, content.get.uri, db) foreach { transaction ⇒
          transaction.completedAt shouldBe None
          transaction.revision should equal(content.get.revision)
        }

        val completer = TestActorRef(new RevaultCompleter(hyperBus, db))
        completer ! completerTask
        val completerResult = expectMsgType[ShardTaskComplete]
        val rc = completerResult.result.asInstanceOf[RevaultCompleterTaskResult]
        rc.path should equal("test-resource-1")
        println(s"rc = $rc")
        rc.transactions should contain(uuid)
        selectTransactions(rc.transactions, "test-resource-1", db) foreach { transaction ⇒
          transaction.completedAt shouldNot be(None)
          transaction.revision should equal(1)
        }*/
      }*/
    }

    "Revault integrated" - {
      "Test revault (integrated)" in {
        val hyperBus = testHyperBus()
        val tk = testKit()
        import tk._
        import system._

        cleanUpCassandra()

        val workerProps = Props(classOf[RevaultWorker], hyperBus, db, 10.seconds)
        val completerProps = Props(classOf[RevaultCompleter], hyperBus, db)
        val workerSettings = Map(
          "revault" →(workerProps, 1),
          "revault-completer" →(completerProps, 1)
        )

        val processor = TestActorRef(new ShardProcessor(workerSettings, "revault"))
        val distributor = TestActorRef(new HyperbusAdapter(processor, db, 20.seconds))
        import eu.inn.hyperbus.akkaservice._
        implicit val timeout = Timeout(20.seconds)
        hyperBus.routeTo[HyperbusAdapter](distributor).futureValue // wait while subscription is completes

        val putEventPromise = Promise[RevaultFeedPut]()
        hyperBus |> { put: RevaultFeedPut ⇒
          Future {
            putEventPromise.success(put)
          }
        }

        Thread.sleep(2000)

        val path = UUID.randomUUID().toString
        whenReady(hyperBus <~ RevaultPut(path, DynamicBody(Text("Hello"))), TestTimeout(10.seconds)) { response ⇒
          response.status should equal(Status.CREATED)
        }

        val putEventFuture = putEventPromise.future
        whenReady(putEventFuture) { putEvent ⇒
          putEvent.method should equal(Method.FEED_PUT)
          putEvent.body should equal(DynamicBody(Text("Hello")))
          putEvent.headers.get(Header.REVISION) shouldNot be(None)
        }

        whenReady(hyperBus <~ RevaultGet(path, EmptyBody), TestTimeout(10.seconds)) { response ⇒
          response.status should equal(Status.OK)
          response.body.content should equal(Text("Hello"))
        }
      }

      "Null patch with revault (integrated)" in {
        val hyperBus = testHyperBus()
        val tk = testKit()
        import tk._
        import system._

        cleanUpCassandra()

        val workerProps = Props(classOf[RevaultWorker], hyperBus, db, 10.seconds)
        val completerProps = Props(classOf[RevaultCompleter], hyperBus, db)
        val workerSettings = Map(
          "revault" →(workerProps, 1),
          "revault-completer" →(completerProps, 1)
        )

        val processor = TestActorRef(new ShardProcessor(workerSettings, "revault"))
        val distributor = TestActorRef(new HyperbusAdapter(processor, db, 20.seconds))
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

        val path = UUID.randomUUID().toString
        whenReady(hyperBus <~ RevaultPut(path, DynamicBody(
          Obj(Map("a" → Text("1"), "b" → Text("2"), "c" → Text("3")))
        )), TestTimeout(10.seconds)) { response ⇒
          response.status should equal(Status.CREATED)
        }

        whenReady(hyperBus <~ RevaultPatch(path, DynamicBody(Obj(Map("b" → Null)))), TestTimeout(10.seconds)) { response ⇒
          response.status should equal(Status.OK)
        }

        val patchEventFuture = patchEventPromise.future
        whenReady(patchEventFuture) { patchEvent ⇒
          patchEvent.method should equal(Method.FEED_PATCH)
          patchEvent.body should equal(DynamicBody(Obj(Map("b" → Null))))
          patchEvent.headers.get(Header.REVISION) shouldNot be(None)
        }

        whenReady(hyperBus <~ RevaultGet(path, EmptyBody), TestTimeout(10.seconds)) { response ⇒
          response.status should equal(Status.OK)
          response.body.content should equal(Obj(Map("a" → Text("1"), "c" → Text("3"))))
        }
      }
    }

    "Recovery" - {
      "HotRecoveryWorker" in {
        val hyperBus = testHyperBus()
        val tk = testKit()
        import tk._

        cleanUpCassandra()

        val worker = TestActorRef(new RevaultWorker(hyperBus, db, 10.seconds))
        val path = "incomplete-" + UUID.randomUUID().toString
        val taskStr1 = StringSerializer.serializeToString(RevaultPut(path,
          DynamicBody(Obj(Map("text" → Text("Test resource value"), "null" → Null)))
        ))
        worker ! RevaultTask("", System.currentTimeMillis() + 10000, taskStr1)
        val completerTask = expectMsgType[RevaultCompleterTask]
        expectMsgType[ShardTaskComplete]

        val transactionUuids = whenReady(db.selectContent(path, "")) { result =>
          result.get.transactionList
        }

        val processorProbe = TestProbe("processor")
        val hotWorkerProps = Props(classOf[HotRecoveryWorker],
          (60 * 1000l, -60 * 1000l), db, processorProbe.ref, 1.seconds, 10.seconds
        )

        val hotWorker = TestActorRef(hotWorkerProps)
        val selfAddress = Address("tcp", "127.0.0.1")
        val shardData = ShardedClusterData(Map(
          selfAddress → ShardMember(ActorSelection(self, ""), ShardMemberStatus.Active, ShardMemberStatus.Active)
        ), selfAddress, ShardMemberStatus.Active)

        // start recovery check
        hotWorker ! UpdateShardStatus(self, Active, shardData)

        val completerTask2 = processorProbe.expectMsgType[RevaultCompleterTask](max = 30.seconds)
        completerTask.documentUri should equal(completerTask2.documentUri)
        processorProbe.reply(CompletionFailedException(completerTask2.documentUri, "Testing worker behavior"))
        val completerTask3 = processorProbe.expectMsgType[RevaultCompleterTask](max = 30.seconds)
        hotWorker ! processorProbe.reply(RevaultCompleterTaskResult(completerTask2.documentUri, transactionUuids))
        gracefulStop(hotWorker, 30 seconds, ShutdownRecoveryWorker).futureValue(TestTimeout(30.seconds))
      }

      "StaleRecoveryWorker" in {
        val hyperBus = testHyperBus()
        val tk = testKit()
        import tk._

        cleanUpCassandra()

        val worker = TestActorRef(new RevaultWorker(hyperBus, db, 10.seconds))
        val path = "incomplete-" + UUID.randomUUID().toString
        val taskStr1 = StringSerializer.serializeToString(RevaultPut(path,
          DynamicBody(Obj(Map("text" → Text("Test resource value"), "null" → Null)))
        ))
        val millis = System.currentTimeMillis()
        worker ! RevaultTask("", System.currentTimeMillis() + 10000, taskStr1)
        val completerTask = expectMsgType[RevaultCompleterTask]
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
        val staleWorkerProps = Props(classOf[StaleRecoveryWorker],
          (60 * 1000l, -60 * 1000l), db, processorProbe.ref, 1.seconds, 2.seconds
        )

        val hotWorker = TestActorRef(staleWorkerProps)
        val selfAddress = Address("tcp", "127.0.0.1")
        val shardData = ShardedClusterData(Map(
          selfAddress → ShardMember(ActorSelection(self, ""), ShardMemberStatus.Active, ShardMemberStatus.Active)
        ), selfAddress, ShardMemberStatus.Active)

        // start recovery check
        hotWorker ! UpdateShardStatus(self, Active, shardData)

        val completerTask2 = processorProbe.expectMsgType[RevaultCompleterTask](max = 30.seconds)
        completerTask.documentUri should equal(completerTask2.documentUri)
        processorProbe.reply(CompletionFailedException(completerTask2.documentUri, "Testing worker behavior"))

        eventually {
          db.selectCheckpoint(transaction.partition).futureValue shouldBe Some(newTransaction.dtQuantum - 1)
        }

        val completerTask3 = processorProbe.expectMsgType[RevaultCompleterTask](max = 30.seconds)
        hotWorker ! processorProbe.reply(RevaultCompleterTaskResult(completerTask2.documentUri, newContent.transactionList))

        eventually {
          db.selectCheckpoint(transaction.partition).futureValue.get shouldBe >(newTransaction.dtQuantum)
        }

        val completerTask4 = processorProbe.expectMsgType[RevaultCompleterTask](max = 30.seconds) // this is abandoned
        hotWorker ! processorProbe.reply(RevaultCompleterTaskResult(completerTask4.documentUri, List()))

        gracefulStop(hotWorker, 30 seconds, ShutdownRecoveryWorker).futureValue(TestTimeout(30.seconds))
      }
    }
  }
  def response(content: String): Response[Body] = StringDeserializer.dynamicResponse(content)
}
