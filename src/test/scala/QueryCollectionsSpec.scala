import eu.inn.binders.value._
import eu.inn.hyperbus.Hyperbus
import eu.inn.hyperbus.model._
import eu.inn.hyperbus.model.utils.{Sort, SortBy}
import eu.inn.hyperstorage.api._
import eu.inn.hyperstorage.db._
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

      reset(db)
      hyperbus
    }

    def setupIndexes(hyperbus: Hyperbus): Unit = {
      (hyperbus <~ HyperStorageIndexPost("collection-1~",
        HyperStorageIndexNew(Some("index1"), Seq.empty, Some("b > 10"))))
        .futureValue.statusCode should equal(Status.CREATED)

      eventually {
        val indexDefUp1 = db.selectIndexDef("collection-1~", "index1").futureValue
        indexDefUp1 shouldBe defined
        indexDefUp1.get.status shouldBe IndexDef.STATUS_NORMAL
      }

      (hyperbus <~ HyperStorageIndexPost("collection-1~",
        HyperStorageIndexNew(Some("index2"), Seq(HyperStorageIndexSortItem("a", order = Some("asc"), fieldType = Some("text"))), Some("b > 10"))))
        .futureValue.statusCode should equal(Status.CREATED)

      eventually {
        val indexDefUp2 = db.selectIndexDef("collection-1~", "index2").futureValue
        indexDefUp2 shouldBe defined
        indexDefUp2.get.status shouldBe IndexDef.STATUS_NORMAL
      }

      (hyperbus <~ HyperStorageIndexPost("collection-1~",
        HyperStorageIndexNew(Some("index3"), Seq(HyperStorageIndexSortItem("a", order = Some("asc"), fieldType = Some("text"))), None)))
        .futureValue.statusCode should equal(Status.CREATED)

      eventually {
        val indexDefUp3 = db.selectIndexDef("collection-1~", "index3").futureValue
        indexDefUp3 shouldBe defined
        indexDefUp3.get.status shouldBe IndexDef.STATUS_NORMAL
      }
      reset(db)
    }

    "Query by id asc" in {
      val hyperbus = setup()
      setupIndexes(hyperbus)
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
      setupIndexes(hyperbus)

      val res = (hyperbus <~ HyperStorageContentGet("collection-1~",
        body = new QueryBuilder() sortBy Seq(SortBy("id", descending = true)) add("size", 50) result()
      )).futureValue
      res.statusCode shouldBe Status.OK
      res.body.content shouldBe ObjV("_embedded" -> ObjV("els" → LstV(c3x, c2x, c1x)))
      verify(db).selectContentCollection("collection-1~", 50, None, false)
    }

    "Query by id asc and filter by id" in {
      val hyperbus = setup()
      setupIndexes(hyperbus)

      val res = (hyperbus <~ HyperStorageContentGet("collection-1~",
        body = new QueryBuilder() sortBy Seq(SortBy("id")) add("size", 50) add("filter", "id >\"item1\"") result()
      )).futureValue
      res.statusCode shouldBe Status.OK
      res.body.content shouldBe ObjV("_embedded" -> ObjV("els" → LstV(c2x, c3x)))
      verify(db).selectContentCollection("collection-1~", 50, Some("item1"), true)
    }

    "Query by id desc and filter by id" in {
      val hyperbus = setup()
      setupIndexes(hyperbus)

      val res = (hyperbus <~ HyperStorageContentGet("collection-1~",
        body = new QueryBuilder() sortBy Seq(SortBy("id", descending = true)) add("size", 50) add("filter", "id <\"item3\"") result()
      )).futureValue
      res.statusCode shouldBe Status.OK
      res.body.content shouldBe ObjV("_embedded" -> ObjV("els" → LstV(c2x, c1x)))
      verify(db).selectContentCollection("collection-1~", 50, Some("item3"), false)
    }

    "Query with filter by some non-index field" in {
      val hyperbus = setup()
      setupIndexes(hyperbus)

      val res = (hyperbus <~ HyperStorageContentGet("collection-1~",
        body = new QueryBuilder() add("size", 2) add("filter", "a =\"way way\"") result()
      )).futureValue
      res.statusCode shouldBe Status.OK
      res.body.content shouldBe ObjV("_embedded" -> ObjV("els" → LstV(c3x)))
      verify(db).selectContentCollection("collection-1~", 2, None, true)
      verify(db).selectContentCollection("collection-1~", 2, Some("item2"), true)
    }

    "Query with filter by some non-index field and descending sorting" in {
      val hyperbus = setup()
      setupIndexes(hyperbus)

      val res = (hyperbus <~ HyperStorageContentGet("collection-1~",
        body = new QueryBuilder() sortBy Seq(SortBy("id", descending = true)) add("size", 2) add("filter", "a =\"hello\"") result()
      )).futureValue
      res.statusCode shouldBe Status.OK
      res.body.content shouldBe ObjV("_embedded" -> ObjV("els" → LstV(c1x)))
      verify(db).selectContentCollection("collection-1~", 2, None, false)
      verify(db).selectContentCollection("collection-1~", 2, Some("item2"), false)
    }

    "Query with filter and sorting by some non-index field (full table scan)" in {
      val hyperbus = setup()

      val res = (hyperbus <~ HyperStorageContentGet("collection-1~",
        body = new QueryBuilder() sortBy Seq(SortBy("a", descending = true)) add("size", 2) add("filter", "a >\"goodbye\"") result()
      )).futureValue
      res.statusCode shouldBe Status.OK
      res.body.content shouldBe ObjV("_embedded" -> ObjV("els" → LstV(c3x, c1x)))
      verify(db).selectContentCollection("collection-1~", 10002, None, true)
    }

    "Query when filter matches index filter and sort order" in {
      val hyperbus = setup()
      setupIndexes(hyperbus)

      val res = (hyperbus <~ HyperStorageContentGet("collection-1~",
        body = new QueryBuilder() sortBy Seq(SortBy("id")) add("size", 50) add("filter", "b > 10") result()
      )).futureValue
      res.statusCode shouldBe Status.OK
      res.body.content shouldBe ObjV("_embedded" -> ObjV("els" → LstV(c1x, c3x)))
      verify(db).selectIndexCollection("index_content", "collection-1~", "index1", Seq.empty, Seq(CkField("item_id",true)), 50)

      val res2 = (hyperbus <~ HyperStorageContentGet("collection-1~",
        body = new QueryBuilder() sortBy Seq(SortBy("a")) add("size", 50) add("filter", "b > 10") result()
      )).futureValue
      res2.statusCode shouldBe Status.OK
      res2.body.content shouldBe ObjV("_embedded" -> ObjV("els" → LstV(c1x, c3x)))
      verify(db).selectIndexCollection("index_content_ta0", "collection-1~", "index2", Seq.empty, Seq(CkField("t0",true)), 50)
    }

    "Query when filter matches index filter and reversed sort order" in {
      val hyperbus = setup()
      setupIndexes(hyperbus)

      val res = (hyperbus <~ HyperStorageContentGet("collection-1~",
        body = new QueryBuilder() sortBy Seq(SortBy("a", descending = true)) add("size", 50) add("filter", "b > 10") result()
      )).futureValue
      res.statusCode shouldBe Status.OK
      res.body.content shouldBe ObjV("_embedded" -> ObjV("els" → LstV(c3x,c1x)))
      verify(db).selectIndexCollection("index_content_ta0", "collection-1~", "index2", Seq.empty, Seq(CkField("t0",false)), 50)

      val res2 = (hyperbus <~ HyperStorageContentGet("collection-1~",
        body = new QueryBuilder() sortBy Seq(SortBy("a", descending = true), SortBy("id", descending = true)) add("size", 50) add("filter", "b > 10") result()
      )).futureValue
      res2.statusCode shouldBe Status.OK
      res2.body.content shouldBe ObjV("_embedded" -> ObjV("els" → LstV(c3x,c1x)))
      verify(db).selectIndexCollection("index_content_ta0", "collection-1~", "index2", Seq.empty, Seq(CkField("t0",false),CkField("item_id",false)), 50)
    }

    "Query when filter partially matches index filter and sort order" in {
      val hyperbus = setup()
      setupIndexes(hyperbus)

      val res = (hyperbus <~ HyperStorageContentGet("collection-1~",
        body = new QueryBuilder() sortBy Seq(SortBy("id")) add("size", 50) add("filter", "b > 12") result()
      )).futureValue
      res.statusCode shouldBe Status.OK
      res.body.content shouldBe ObjV("_embedded" -> ObjV("els" → LstV(c1x)))
      verify(db).selectIndexCollection("index_content", "collection-1~", "index1", Seq.empty, Seq(CkField("item_id",true)), 50)
    }

    "Query when filter partially matches index filter and sort order with CK field filter" in {
      val hyperbus = setup()
      setupIndexes(hyperbus)

      val res = (hyperbus <~ HyperStorageContentGet("collection-1~",
        body = new QueryBuilder() sortBy Seq(SortBy("a")) add("size", 50) add("filter", "b > 10 and a > \"hello\"") result()
      )).futureValue
      res.statusCode shouldBe Status.OK
      res.body.content shouldBe ObjV("_embedded" -> ObjV("els" → LstV(c3x)))
      verify(db).selectIndexCollection("index_content_ta0", "collection-1~", "index2", Seq(FieldFilter("t0", Text("hello"), FilterGt)), Seq(CkField("t0",true)), 50)

      val res2 = (hyperbus <~ HyperStorageContentGet("collection-1~",
        body = new QueryBuilder() sortBy Seq(SortBy("a"),SortBy("id")) add("size", 50) add("filter", "b > 10 and a = \"hello\" and id > \"item2\"") result()
      )).futureValue
      res2.statusCode shouldBe Status.OK
      res2.body.content shouldBe ObjV("_embedded" -> ObjV("els" → Lst.empty))
      verify(db).selectIndexCollection("index_content_ta0", "collection-1~", "index2", Seq(FieldFilter("t0", Text("hello"), FilterEq),FieldFilter("item_id", Text("item2"), FilterGt)), Seq(CkField("t0",true),CkField("item_id",true)), 50)
    }

    "Query when sort order matches with CK fields (skipping unmatched to the filter)" in {
      val hyperbus = setup()
      setupIndexes(hyperbus)

      val res = (hyperbus <~ HyperStorageContentGet("collection-1~",
        body = new QueryBuilder() sortBy Seq(SortBy("a")) add("size", 2) add("filter", "b < 50") result()
      )).futureValue
      res.statusCode shouldBe Status.OK
      res.body.content shouldBe ObjV("_embedded" -> ObjV("els" → LstV(c2x,c3x)))
      verify(db).selectIndexCollection("index_content_ta0", "collection-1~", "index3", Seq.empty, Seq(CkField("t0",true)), 2)
      verify(db).selectIndexCollection("index_content_ta0", "collection-1~", "index3", Seq(FieldFilter("t0", Text("hello"), FilterGt)), Seq(CkField("t0",true)), 2)
    }

    "Query when sort order matches with CK fields (skipping unmatched to the filter) with query-filter-ck-fields" in {
      val hyperbus = setup()
      setupIndexes(hyperbus)

      val res = (hyperbus <~ HyperStorageContentGet("collection-1~",
        body = new QueryBuilder() sortBy Seq(SortBy("a")) add("size", 2) add("filter", "b < 50 and a < \"zzz\" ") result()
      )).futureValue
      res.statusCode shouldBe Status.OK
      res.body.content shouldBe ObjV("_embedded" -> ObjV("els" → LstV(c2x,c3x)))
      verify(db).selectIndexCollection("index_content_ta0", "collection-1~", "index3", Seq(FieldFilter("t0", Text("zzz"), FilterLt)), Seq(CkField("t0",true)), 2)
      verify(db).selectIndexCollection("index_content_ta0", "collection-1~", "index3", Seq(FieldFilter("t0", Text("hello"), FilterEq),FieldFilter("item_id", Text("item1"), FilterGt)), Seq(CkField("t0",true)), 2)
    }

    "Query when sort order matches with CK fields (skipping unmatched to the filter) with query-filter-ck-fields / reversed" in {
      val hyperbus = setup()
      setupIndexes(hyperbus)

      val res = (hyperbus <~ HyperStorageContentGet("collection-1~",
        body = new QueryBuilder() sortBy Seq(SortBy("a", descending = true)) add("size", 2) add("filter", "b < 50 and a > \"aaa\" ") result()
      )).futureValue
      res.statusCode shouldBe Status.OK
      res.body.content shouldBe ObjV("_embedded" -> ObjV("els" → LstV(c3x,c2x)))
      verify(db).selectIndexCollection("index_content_ta0", "collection-1~", "index3", Seq(FieldFilter("t0", Text("aaa"), FilterGt)), Seq(CkField("t0",false)), 2)
      verify(db).selectIndexCollection("index_content_ta0", "collection-1~", "index3", Seq(FieldFilter("t0", Text("hello"), FilterEq),FieldFilter("item_id", Text("item1"), FilterLt)), Seq(CkField("t0",false)), 2)
    }
  }
}
