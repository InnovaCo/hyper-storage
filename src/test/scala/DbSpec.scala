import java.util.Date

import eu.inn.binders.value.Text
import eu.inn.hyperstorage.db._
import org.scalatest.concurrent.Eventually
import org.scalatest.{FreeSpec, Matchers}

class DbSpec extends FreeSpec with Matchers with CassandraFixture
  with TestHelpers
  with Eventually {
  "DbSpec" - {
    "Index collection with ordering by id" in {
      cleanUpCassandra()
      db.insertIndexItem("index_content", Seq.empty, IndexContent(
        "test~", "x1", "i1", 1l, Some("{}"), new Date(), None
      ))
      db.insertIndexItem("index_content", Seq.empty, IndexContent(
        "test~", "x1", "i2", 1l, Some("{}"), new Date(), None
      ))
      db.insertIndexItem("index_content", Seq.empty, IndexContent(
        "test~", "x1", "i3", 1l, Some("{}"), new Date(), None
      ))
      val ca = db.selectIndexCollection("index_content", "test~", "x1", Seq.empty,
        Seq(CkField("item_id", ascending = true)), 10).futureValue.toSeq
      ca(0).itemId shouldBe "i1"
      ca(1).itemId shouldBe "i2"
      ca(2).itemId shouldBe "i3"

      val cd = db.selectIndexCollection("index_content", "test~", "x1",
        Seq.empty,
        Seq(CkField("item_id", ascending = false)),
        10).futureValue.toSeq
      cd(0).itemId shouldBe "i3"
      cd(1).itemId shouldBe "i2"
      cd(2).itemId shouldBe "i1"

      val ca2 = db.selectIndexCollection("index_content", "test~", "x1",
        Seq(FieldFilter("item_id", Text("i1"), FilterGt)),
        Seq(CkField("item_id", ascending = true)),
        10).futureValue.toSeq
      ca2(0).itemId shouldBe "i2"
      ca2(1).itemId shouldBe "i3"
      ca2.size shouldBe 2

      val cd2 = db.selectIndexCollection("index_content", "test~", "x1",
        Seq(FieldFilter("item_id", Text("i3"), FilterLt)),
        Seq(CkField("item_id", ascending = false)),
        10).futureValue.toSeq
      cd2(0).itemId shouldBe "i2"
      cd2(1).itemId shouldBe "i1"
      cd2.size shouldBe 2
    }

    "Index collection with ordering by text field" in {
      cleanUpCassandra()
      db.insertIndexItem("index_content_ta0", Seq("t0" → "aa00"), IndexContent(
        "test~", "x1", "i1", 1l, Some("{}"), new Date(), None
      ))
      db.insertIndexItem("index_content_ta0", Seq("t0" → "aa01"), IndexContent(
        "test~", "x1", "i2", 1l, Some("{}"), new Date(), None
      ))
      db.insertIndexItem("index_content_ta0", Seq("t0" → "aa02"), IndexContent(
        "test~", "x1", "i3", 1l, Some("{}"), new Date(), None
      ))
      db.insertIndexItem("index_content_ta0", Seq("t0" → "aa02"), IndexContent(
        "test~", "x1", "i4", 1l, Some("{}"), new Date(), None
      ))
      val ca = db.selectIndexCollection("index_content_ta0", "test~", "x1", Seq.empty, Seq.empty, 10).futureValue.toSeq
      ca(0).itemId shouldBe "i1"
      ca(1).itemId shouldBe "i2"
      ca(2).itemId shouldBe "i3"
      ca(3).itemId shouldBe "i4"

      val cd = db.selectIndexCollection("index_content_ta0", "test~", "x1",
        Seq.empty,
        Seq(CkField("t0", ascending = false)),
        10).futureValue.toSeq
      cd(0).itemId shouldBe "i4"
      cd(1).itemId shouldBe "i3"
      cd(2).itemId shouldBe "i2"
      cd(3).itemId shouldBe "i1"

      val ca2 = db.selectIndexCollection("index_content_ta0", "test~", "x1",
        Seq(FieldFilter("t0", Text("aa00"), FilterGt)),
        Seq(CkField("t0", ascending = true)),
        10).futureValue.toSeq
      ca2(0).itemId shouldBe "i2"
      ca2(1).itemId shouldBe "i3"
      ca2(2).itemId shouldBe "i4"
      ca2.size shouldBe 3

      val cd2 = db.selectIndexCollection("index_content_ta0", "test~", "x1",
        Seq(FieldFilter("t0", Text("aa02"), FilterLt)),
        Seq(CkField("t0", ascending = false)),
        10).futureValue.toSeq
      cd2(0).itemId shouldBe "i2"
      cd2(1).itemId shouldBe "i1"
      cd2.size shouldBe 2

      val ca3 = db.selectIndexCollection("index_content_ta0", "test~", "x1",
        Seq(FieldFilter("t0", Text("aa02"), FilterEq), FieldFilter("item_id", Text("i3"), FilterGt)),
        Seq(CkField("t0", ascending = true), CkField("item_id", ascending = true)),
        10).futureValue.toSeq
      ca3(0).itemId shouldBe "i4"
      ca3.size shouldBe 1

      val cd3 = db.selectIndexCollection("index_content_ta0", "test~", "x1",
        Seq(FieldFilter("t0", Text("aa02"), FilterEq), FieldFilter("item_id", Text("i4"), FilterLt)),
        Seq(CkField("t0", ascending = false), CkField("item_id", ascending = false)),
        10).futureValue.toSeq
      cd3(0).itemId shouldBe "i3"
      cd3.size shouldBe 1
    }
  }
}
