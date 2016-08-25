import eu.inn.hyperbus.model.utils.SortBy
import eu.inn.hyperstorage.api.{HyperStorageIndexSortItem, HyperStorageIndexSortOrder}
import eu.inn.hyperstorage.indexing.OrderWeigher
import org.scalatest.{FreeSpec, Matchers}



class OrderWeigherTest extends FreeSpec with Matchers {
  "OrderWeigher" - {
    "equal orders should be 10" in {
      OrderWeigher.weighOrdering(Seq(SortBy("a", descending = false)), Seq(HyperStorageIndexSortItem("a",None,None))) shouldBe 10
      OrderWeigher.weighOrdering(Seq(SortBy("a", descending = false)), Seq(HyperStorageIndexSortItem("a",None,Some(HyperStorageIndexSortOrder.ASC)))) shouldBe 10
      OrderWeigher.weighOrdering(Seq(SortBy("a"), SortBy("b", descending = true)), Seq(HyperStorageIndexSortItem("a",None,None),HyperStorageIndexSortItem("b",None,Some(HyperStorageIndexSortOrder.DESC)))) shouldBe 10
    }

    "empty query orders should be 0" in {
      OrderWeigher.weighOrdering(Seq.empty, Seq(HyperStorageIndexSortItem("a",None,None))) shouldBe 0
    }

    "reverse index order should be 5" in {
      OrderWeigher.weighOrdering(Seq(SortBy("a", descending = true)), Seq(HyperStorageIndexSortItem("a",None,None))) shouldBe 5
      OrderWeigher.weighOrdering(Seq(SortBy("a", descending = true)), Seq(HyperStorageIndexSortItem("a",None,Some(HyperStorageIndexSortOrder.ASC)))) shouldBe 5
      OrderWeigher.weighOrdering(Seq(SortBy("a", descending = true), SortBy("b")), Seq(HyperStorageIndexSortItem("a",None,None),HyperStorageIndexSortItem("b",None,Some(HyperStorageIndexSortOrder.DESC)))) shouldBe 5
      OrderWeigher.weighOrdering(Seq(SortBy("a")), Seq(HyperStorageIndexSortItem("a",None,Some(HyperStorageIndexSortOrder.DESC)))) shouldBe 5
      OrderWeigher.weighOrdering(Seq(SortBy("a")), Seq(HyperStorageIndexSortItem("a",None,Some(HyperStorageIndexSortOrder.DESC)))) shouldBe 5
      OrderWeigher.weighOrdering(Seq(SortBy("a"), SortBy("b", descending = true)), Seq(HyperStorageIndexSortItem("a",None,Some(HyperStorageIndexSortOrder.DESC)),HyperStorageIndexSortItem("b",None,Some(HyperStorageIndexSortOrder.ASC)))) shouldBe 5
    }

    "partially equal order should be 3" in {
      OrderWeigher.weighOrdering(Seq(SortBy("a", descending = true), SortBy("b")), Seq(HyperStorageIndexSortItem("a",None,None),HyperStorageIndexSortItem("b",None,Some(HyperStorageIndexSortOrder.ASC)))) shouldBe 3
      OrderWeigher.weighOrdering(Seq(SortBy("a"), SortBy("b", descending = true)), Seq(HyperStorageIndexSortItem("a",None,Some(HyperStorageIndexSortOrder.DESC)),HyperStorageIndexSortItem("b",None,Some(HyperStorageIndexSortOrder.DESC)))) shouldBe 3
      OrderWeigher.weighOrdering(Seq(SortBy("a"), SortBy("b", descending = true)), Seq(HyperStorageIndexSortItem("a",None,Some(HyperStorageIndexSortOrder.DESC)))) shouldBe 3
    }

    "unequal order should be -10" in {
      OrderWeigher.weighOrdering(Seq(SortBy("z", descending = false)), Seq(HyperStorageIndexSortItem("a",None,None))) shouldBe -10
      OrderWeigher.weighOrdering(Seq(SortBy("x", descending = true), SortBy("b")), Seq(HyperStorageIndexSortItem("a",None,None),HyperStorageIndexSortItem("b",None,Some(HyperStorageIndexSortOrder.ASC)))) shouldBe -10
      OrderWeigher.weighOrdering(Seq(SortBy("y"), SortBy("b", descending = true)), Seq(HyperStorageIndexSortItem("a",None,Some(HyperStorageIndexSortOrder.DESC)),HyperStorageIndexSortItem("b",None,Some(HyperStorageIndexSortOrder.DESC)))) shouldBe -10
    }
  }
}
