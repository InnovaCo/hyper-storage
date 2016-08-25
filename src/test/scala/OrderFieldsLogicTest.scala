import eu.inn.hyperbus.model.utils.SortBy
import eu.inn.hyperstorage.api.{HyperStorageIndexSortFieldType, HyperStorageIndexSortItem, HyperStorageIndexSortOrder}
import eu.inn.hyperstorage.db.CkField
import eu.inn.hyperstorage.indexing.OrderFieldsLogic
import org.scalatest.{FreeSpec, Matchers}



class OrderFieldsLogicTest extends FreeSpec with Matchers {
  "OrderFieldsLogic - weighOrdering" - {
    "equal orders should be 10" in {
      OrderFieldsLogic.weighOrdering(Seq(SortBy("a", descending = false)), Seq(HyperStorageIndexSortItem("a",None,None))) shouldBe 10
      OrderFieldsLogic.weighOrdering(Seq(SortBy("a", descending = false)), Seq(HyperStorageIndexSortItem("a",None,Some(HyperStorageIndexSortOrder.ASC)))) shouldBe 10
      OrderFieldsLogic.weighOrdering(Seq(SortBy("a"), SortBy("b", descending = true)), Seq(HyperStorageIndexSortItem("a",None,None),HyperStorageIndexSortItem("b",None,Some(HyperStorageIndexSortOrder.DESC)))) shouldBe 10
    }

    "empty query orders should be 0" in {
      OrderFieldsLogic.weighOrdering(Seq.empty, Seq(HyperStorageIndexSortItem("a",None,None))) shouldBe 0
    }

    "reverse index order should be 5" in {
      OrderFieldsLogic.weighOrdering(Seq(SortBy("a", descending = true)), Seq(HyperStorageIndexSortItem("a",None,None))) shouldBe 5
      OrderFieldsLogic.weighOrdering(Seq(SortBy("a", descending = true)), Seq(HyperStorageIndexSortItem("a",None,Some(HyperStorageIndexSortOrder.ASC)))) shouldBe 5
      OrderFieldsLogic.weighOrdering(Seq(SortBy("a", descending = true), SortBy("b")), Seq(HyperStorageIndexSortItem("a",None,None),HyperStorageIndexSortItem("b",None,Some(HyperStorageIndexSortOrder.DESC)))) shouldBe 5
      OrderFieldsLogic.weighOrdering(Seq(SortBy("a")), Seq(HyperStorageIndexSortItem("a",None,Some(HyperStorageIndexSortOrder.DESC)))) shouldBe 5
      OrderFieldsLogic.weighOrdering(Seq(SortBy("a"), SortBy("b", descending = true)), Seq(HyperStorageIndexSortItem("a",None,Some(HyperStorageIndexSortOrder.DESC)),HyperStorageIndexSortItem("b",None,Some(HyperStorageIndexSortOrder.ASC)))) shouldBe 5
    }

    "partially equal order should be 3" in {
      OrderFieldsLogic.weighOrdering(Seq(SortBy("a", descending = true), SortBy("b")), Seq(HyperStorageIndexSortItem("a",None,None),HyperStorageIndexSortItem("b",None,Some(HyperStorageIndexSortOrder.ASC)))) shouldBe 3
      OrderFieldsLogic.weighOrdering(Seq(SortBy("a"), SortBy("b", descending = true)), Seq(HyperStorageIndexSortItem("a",None,Some(HyperStorageIndexSortOrder.DESC)),HyperStorageIndexSortItem("b",None,Some(HyperStorageIndexSortOrder.DESC)))) shouldBe 3
      OrderFieldsLogic.weighOrdering(Seq(SortBy("a"), SortBy("b", descending = true)), Seq(HyperStorageIndexSortItem("a",None,Some(HyperStorageIndexSortOrder.DESC)))) shouldBe 3
    }

    "unequal order should be -10" in {
      OrderFieldsLogic.weighOrdering(Seq(SortBy("z", descending = false)), Seq(HyperStorageIndexSortItem("a",None,None))) shouldBe -10
      OrderFieldsLogic.weighOrdering(Seq(SortBy("x", descending = true), SortBy("b")), Seq(HyperStorageIndexSortItem("a",None,None),HyperStorageIndexSortItem("b",None,Some(HyperStorageIndexSortOrder.ASC)))) shouldBe -10
      OrderFieldsLogic.weighOrdering(Seq(SortBy("y"), SortBy("b", descending = true)), Seq(HyperStorageIndexSortItem("a",None,Some(HyperStorageIndexSortOrder.DESC)),HyperStorageIndexSortItem("b",None,Some(HyperStorageIndexSortOrder.DESC)))) shouldBe -10
    }
  }

  "OrderFieldsLogic - extractIndexSortFields" - {
    "equal orders should be extracted totally" in {
      OrderFieldsLogic.extractIndexSortFields(Seq(SortBy("a", descending = false)), Seq(HyperStorageIndexSortItem("a",None,None))) shouldBe Seq(CkField("t0", ascending=true))
      OrderFieldsLogic.extractIndexSortFields(Seq(SortBy("a", descending = false)), Seq(HyperStorageIndexSortItem("a",None,Some(HyperStorageIndexSortOrder.ASC)))) shouldBe Seq(CkField("t0", ascending=true))
      OrderFieldsLogic.extractIndexSortFields(Seq(SortBy("a"), SortBy("b", descending = true)), Seq(HyperStorageIndexSortItem("a",None,None),HyperStorageIndexSortItem("b",Some(HyperStorageIndexSortFieldType.DECIMAL),Some(HyperStorageIndexSortOrder.DESC)))) shouldBe Seq(CkField("t0", ascending=true), CkField("d1", ascending=false))
    }

    "empty query orders should be Seq.empty" in {
      OrderFieldsLogic.extractIndexSortFields(Seq.empty, Seq(HyperStorageIndexSortItem("a",None,None))) shouldBe Seq.empty
    }

    "reverse index order be extracted totally" in {
      OrderFieldsLogic.extractIndexSortFields(Seq(SortBy("a", descending = true)), Seq(HyperStorageIndexSortItem("a",None,None))) shouldBe Seq(CkField("t0", ascending=false))
      OrderFieldsLogic.extractIndexSortFields(Seq(SortBy("a", descending = true)), Seq(HyperStorageIndexSortItem("a",None,Some(HyperStorageIndexSortOrder.ASC)))) shouldBe Seq(CkField("t0", ascending=false))
      OrderFieldsLogic.extractIndexSortFields(Seq(SortBy("a", descending = true), SortBy("b")), Seq(HyperStorageIndexSortItem("a",None,None),HyperStorageIndexSortItem("b",None,Some(HyperStorageIndexSortOrder.DESC)))) shouldBe Seq(CkField("t0", ascending=false), CkField("t1", ascending=true))
      OrderFieldsLogic.extractIndexSortFields(Seq(SortBy("a")), Seq(HyperStorageIndexSortItem("a",None,Some(HyperStorageIndexSortOrder.DESC)))) shouldBe Seq(CkField("t0", ascending=true))
      OrderFieldsLogic.extractIndexSortFields(Seq(SortBy("a"), SortBy("b", descending = true)), Seq(HyperStorageIndexSortItem("a",None,Some(HyperStorageIndexSortOrder.DESC)),HyperStorageIndexSortItem("b",None,Some(HyperStorageIndexSortOrder.ASC)))) shouldBe Seq(CkField("t0", ascending=true), CkField("t1", ascending=false))
    }

    "partially equal order should be extracted partially" in {
      OrderFieldsLogic.extractIndexSortFields(Seq(SortBy("a", descending = true), SortBy("b")), Seq(HyperStorageIndexSortItem("a",None,None),HyperStorageIndexSortItem("b",None,Some(HyperStorageIndexSortOrder.ASC)))) shouldBe Seq(CkField("t0", ascending=false))
      OrderFieldsLogic.extractIndexSortFields(Seq(SortBy("a"), SortBy("b", descending = true)), Seq(HyperStorageIndexSortItem("a",None,Some(HyperStorageIndexSortOrder.DESC)),HyperStorageIndexSortItem("b",None,Some(HyperStorageIndexSortOrder.DESC)))) shouldBe Seq(CkField("t0", ascending=true))
      OrderFieldsLogic.extractIndexSortFields(Seq(SortBy("a"), SortBy("b", descending = true)), Seq(HyperStorageIndexSortItem("a",None,Some(HyperStorageIndexSortOrder.DESC)))) shouldBe Seq(CkField("t0", ascending=true))
    }

    "unequal order should extract Seq.empty" in {
      OrderFieldsLogic.extractIndexSortFields(Seq(SortBy("z", descending = false)), Seq(HyperStorageIndexSortItem("a",None,None))) shouldBe Seq.empty
      OrderFieldsLogic.extractIndexSortFields(Seq(SortBy("x", descending = true), SortBy("b")), Seq(HyperStorageIndexSortItem("a",None,None),HyperStorageIndexSortItem("b",None,Some(HyperStorageIndexSortOrder.ASC)))) shouldBe Seq.empty
      OrderFieldsLogic.extractIndexSortFields(Seq(SortBy("y"), SortBy("b", descending = true)), Seq(HyperStorageIndexSortItem("a",None,Some(HyperStorageIndexSortOrder.DESC)),HyperStorageIndexSortItem("b",None,Some(HyperStorageIndexSortOrder.DESC)))) shouldBe Seq.empty
    }
  }
}
