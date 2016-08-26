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

class HyperStorageSpec extends FreeSpec
  with Matchers
  with ScalaFutures
  with CassandraFixture
  with TestHelpers
  with Eventually {

  import ContentLogic._

  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(10000, Millis)))

  "HyperStorageSpec" - {
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
}
