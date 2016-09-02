import java.util.UUID

import akka.testkit.TestActorRef
import akka.util.Timeout
import eu.inn.binders.value._
import eu.inn.hyperbus.model._
import eu.inn.hyperbus.model.utils.{Sort, SortBy}
import eu.inn.hyperbus.serialization.StringSerializer
import eu.inn.hyperstorage._
import eu.inn.hyperstorage.api._
import eu.inn.hyperstorage.db.IndexDef
import eu.inn.hyperstorage.sharding._
import eu.inn.hyperstorage.workers.primary.PrimaryWorker
import eu.inn.hyperstorage.workers.secondary.{SecondaryWorker, SecondaryWorker$}
import mock.FaultClientTransport
import org.scalatest.concurrent.PatienceConfiguration.{Timeout ⇒ TestTimeout}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FreeSpec, Matchers}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

class IntegratedSpec extends FreeSpec
  with Matchers
  with ScalaFutures
  with CassandraFixture
  with TestHelpers
  with Eventually {

  import ContentLogic._
  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(10000, Millis)))

  "HyperStorageIntegratedSpec" - {
    "Test hyper-storage PUT+GET+Event" in {
      val hyperbus = testHyperbus()
      val tk = testKit()
      import tk._
      import system._

      cleanUpCassandra()

      val workerProps = PrimaryWorker.props(hyperbus, db, tracker, 10.seconds)
      val secondaryWorkerProps = SecondaryWorker.props(hyperbus, db, tracker, self)
      val workerSettings = Map(
        "hyper-storage-primary-worker" → (workerProps, 1, "pgw-"),
        "hyper-storage-secondary-worker" → (secondaryWorkerProps, 1, "sgw-")
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

      val workerProps = PrimaryWorker.props(hyperbus, db, tracker, 10.seconds)
      val secondaryWorkerProps = SecondaryWorker.props(hyperbus, db, tracker, self)
      val workerSettings = Map(
        "hyper-storage-primary-worker" → (workerProps, 1, "pgw-"),
        "hyper-storage-secondary-worker" → (secondaryWorkerProps, 1, "sgw-")
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

      val workerProps = PrimaryWorker.props(hyperbus, db, tracker, 10.seconds)
      val secondaryWorkerProps = SecondaryWorker.props(hyperbus, db, tracker, self)
      val workerSettings = Map(
        "hyper-storage-primary-worker" → (workerProps, 1, "pgw-"),
        "hyper-storage-secondary-worker" → (secondaryWorkerProps, 1, "sgw-")
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
      val c2 = ObjV("a" → "goodbye", "b" → 654321)
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

      val workerProps = PrimaryWorker.props(hyperbus, db, tracker, 10.seconds)
      val secondaryWorkerProps = SecondaryWorker.props(hyperbus, db, tracker, self)
      val workerSettings = Map(
        "hyper-storage-primary-worker" → (workerProps, 1, "pgw-"),
        "hyper-storage-secondary-worker" → (secondaryWorkerProps, 1, "sgw-")
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
      val c2 = ObjV("a" → "goodbye", "b" → Number(654321))

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
        body = new QueryBuilder() add("size", 50) sortBy (Seq(SortBy("id", false))) result())
      whenReady(f5) { response ⇒
        response.statusCode should equal(Status.OK)
        response.body.content should equal(
          ObjV("_embedded" -> ObjV("els" → LstV(c1x,c2x)))
        )
      }
    }
  }
}
