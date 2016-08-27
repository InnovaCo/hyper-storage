import eu.inn.binders.value._
import eu.inn.hyperbus.Hyperbus
import eu.inn.hyperbus.model._
import eu.inn.hyperbus.model.utils.{Sort, SortBy}
import eu.inn.hyperstorage.api._
import eu.inn.hyperstorage.db.Db
import org.scalatest.concurrent.PatienceConfiguration.{Timeout ⇒ TestTimeout}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FreeSpec, Matchers}
import org.mockito.Mockito._

class QueryCollectionsSpec extends FreeSpec
  with Matchers
  with ScalaFutures
  with CassandraFixture
  with TestHelpers
  with Eventually {

  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(10000, Millis)))

  "Query collection" - {
    val c1 = ObjV("a" → "hello", "b" → 100500)
    val c1x = Obj(c1.asMap + "id" → "item1")
    val c2 = ObjV("a" → "goodbye", "b" → 1)
    val c2x = Obj(c2.asMap + "id" → "item2")
    val c3 = ObjV("a" → "way way", "b" → 12)
    val c3x = Obj(c3.asMap + "id" → "item3")
    import Sort._

    def setup(): Hyperbus = {
      cleanUpCassandra()
      val hyperbus = integratedHyperbus(db)
      val f1 = hyperbus <~ HyperStorageContentPut("collection-1~/item1", DynamicBody(c1))
      f1.futureValue.statusCode shouldBe Status.CREATED

      val f2 = hyperbus <~ HyperStorageContentPut("collection-1~/item2", DynamicBody(c2))
      f2.futureValue.statusCode shouldBe Status.CREATED

      val f3 = hyperbus <~ HyperStorageContentPut("collection-1~/item3", DynamicBody(c3))
      f3.futureValue.statusCode shouldBe Status.CREATED
      hyperbus
    }

    "Query by id asc" in {
      val hyperbus = setup()
      // query by id asc
      val res = (hyperbus <~ HyperStorageContentGet("collection-1~",
        body = new QueryBuilder() sortBy Seq(SortBy("id")) add("size", 50) result()
      )).futureValue
      res.statusCode shouldBe Status.OK
      res.body.content shouldBe ObjV("_embedded" -> ObjV("els" → LstV(c1x, c2x, c3x)))
      verify(db).selectContentCollection("collection-1~", 50, None, true)
    }

    "Query by id desc" in {
      val hyperbus = setup()

      val res = (hyperbus <~ HyperStorageContentGet("collection-1~",
        body = new QueryBuilder() sortBy Seq(SortBy("id", descending = true)) add("size", 50) result()
      )).futureValue
      res.statusCode shouldBe Status.OK
      res.body.content shouldBe ObjV("_embedded" -> ObjV("els" → LstV(c3x, c2x, c1x)))
      verify(db).selectContentCollection("collection-1~", 50, None, false)
    }

    "Query by id asc and filter by id" in {
      val hyperbus = setup()

      val res = (hyperbus <~ HyperStorageContentGet("collection-1~",
        body = new QueryBuilder() sortBy Seq(SortBy("id")) add("size", 50) add("filter", "id >\"item1\"") result()
      )).futureValue
      res.statusCode shouldBe Status.OK
      res.body.content shouldBe ObjV("_embedded" -> ObjV("els" → LstV(c2x, c3x)))
      verify(db).selectContentCollection("collection-1~", 50, Some("item1"), true)
    }

    "Query by id desc and filter by id" in {
      val hyperbus = setup()

      val res = (hyperbus <~ HyperStorageContentGet("collection-1~",
        body = new QueryBuilder() sortBy Seq(SortBy("id", descending = true)) add("size", 50) add("filter", "id <\"item3\"") result()
      )).futureValue
      res.statusCode shouldBe Status.OK
      res.body.content shouldBe ObjV("_embedded" -> ObjV("els" → LstV(c2x, c1x)))
      verify(db).selectContentCollection("collection-1~", 50, Some("item3"), false)
    }

    "Query with filter by some nonindex field" in {
      val hyperbus = setup()

      val res = (hyperbus <~ HyperStorageContentGet("collection-1~",
        body = new QueryBuilder() add("size", 2) add("filter", "a =\"way way\"") result()
      )).futureValue
      res.statusCode shouldBe Status.OK
      res.body.content shouldBe ObjV("_embedded" -> ObjV("els" → LstV(c3x)))
      verify(db).selectContentCollection("collection-1~", 2, None, true)
      verify(db).selectContentCollection("collection-1~", 2, Some("item2"), true)
    }
  }
}
