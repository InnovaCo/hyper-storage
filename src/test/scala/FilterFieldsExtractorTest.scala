import eu.inn.binders.value.{Number, Text}
import eu.inn.hyperstorage.api.{HyperStorageIndexSortFieldType, HyperStorageIndexSortItem, HyperStorageIndexSortOrder}
import eu.inn.hyperstorage.db._
import eu.inn.hyperstorage.indexing.FieldFiltersExtractor
import eu.inn.parser.HParser
import org.scalatest.{FreeSpec, Matchers}



class FilterFieldsExtractorTest extends FreeSpec with Matchers {
  "FilterFieldsExtractor" - {
    "single gt filter field" in {
      val expression = HParser(s""" id > "10" """).get
      val sortByFields =  Seq(HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)))
      val ff = new FieldFiltersExtractor(sortByFields).extract(expression)
      ff shouldBe Seq(FieldFilter("item_id", Text("10"), FilterGt))
    }

    "single lt filter field" in {
      val expression = HParser(s""" id < "10" """).get
      val sortByFields =  Seq(HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)))
      val ff = new FieldFiltersExtractor(sortByFields).extract(expression)
      ff shouldBe Seq(FieldFilter("item_id", Text("10"), FilterLt))
    }

    "single gteq filter field" in {
      val expression = HParser(s""" id >= "10" """).get
      val sortByFields =  Seq(HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)))
      val ff = new FieldFiltersExtractor(sortByFields).extract(expression)
      ff shouldBe Seq(FieldFilter("item_id", Text("10"), FilterGtEq))
    }

    "single lteq filter field" in {
      val expression = HParser(s""" id <= "10" """).get
      val sortByFields =  Seq(HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)))
      val ff = new FieldFiltersExtractor(sortByFields).extract(expression)
      ff shouldBe Seq(FieldFilter("item_id", Text("10"), FilterLtEq))
    }

    "single eq filter field" in {
      val expression = HParser(s""" id = "10" """).get
      val sortByFields =  Seq(HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)))
      val ff = new FieldFiltersExtractor(sortByFields).extract(expression)
      ff shouldBe Seq(FieldFilter("item_id", Text("10"), FilterEq))
    }

    "single gt reversed filter field" in {
      val expression = HParser(s""" "10" < id """).get
      val sortByFields =  Seq(HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)))
      val ff = new FieldFiltersExtractor(sortByFields).extract(expression)
      ff shouldBe Seq(FieldFilter("item_id", Text("10"), FilterGt))
    }

    "gt filter field with some other field" in {
      val expression = HParser(s""" id > "10" and x < 5 """).get
      val sortByFields =  Seq(HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)))
      val ff = new FieldFiltersExtractor(sortByFields).extract(expression)
      ff shouldBe Seq(FieldFilter("item_id", Text("10"), FilterGt))
    }

    "eq filter field with some other fields" in {
      val expression = HParser(s""" id = "10" and x < 5 and z*3 > 24""").get
      val sortByFields =  Seq(HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)))
      val ff = new FieldFiltersExtractor(sortByFields).extract(expression)
      ff shouldBe Seq(FieldFilter("item_id", Text("10"), FilterEq))
    }

    "eq filter multiple fields with some other fields" in {
      val expression = HParser(s""" id = "10" and x < 5 and z*3 > 24 and y = 12""").get
      val sortByFields =  Seq(
        HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)),
        HyperStorageIndexSortItem("x", Some(HyperStorageIndexSortFieldType.DECIMAL), Some(HyperStorageIndexSortOrder.ASC))
      )
      val ff = new FieldFiltersExtractor(sortByFields).extract(expression)
      ff shouldBe Seq(FieldFilter("t0", Text("10"), FilterEq), FieldFilter("d1", Number(5), FilterLt))
    }

    "gt filter field with or expression shouldn't match" in {
      val expression = HParser(s""" id > "10" or x < 5 """).get
      val sortByFields =  Seq(HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)))
      val ff = new FieldFiltersExtractor(sortByFields).extract(expression)
      ff shouldBe Seq.empty
    }
  }
}
