import eu.inn.binders.value.{Obj, ObjV, Value}
import eu.inn.hyperbus.model.utils.SortBy
import eu.inn.hyperstorage.CollectionOrdering
import org.scalatest.{FreeSpec, Matchers}


class CollectionOrderingTest extends FreeSpec with Matchers {
  "CollectionOrdering" - {
    val c1 = ObjV("a" → "hello", "b" → 100500, "c" → 10)
    val c1x = Obj(c1.asMap + "id" → "item1")
    val c2 = ObjV("a" → "goodbye", "b" → 1, "c" → 20)
    val c2x = Obj(c2.asMap + "id" → "item2")
    val c3 = ObjV("a" → "way way", "b" → 12, "c" → 10)
    val c3x = Obj(c3.asMap + "id" → "item3")

    "sort" - {
      val list = List(c1x,c2x,c3x)
      val ordering = new CollectionOrdering(Seq(SortBy("a")))
      list.sorted(ordering) shouldBe List(c2x,c1x,c3x)
    }

    "sort descending" - {
      val list = List(c1x,c2x,c3x)
      val ordering = new CollectionOrdering(Seq(SortBy("a", descending=true)))
      list.sorted(ordering) shouldBe List(c3x,c1x,c2x)
    }

    "sort two fields" - {
      val list = List(c1x,c2x,c3x)
      val ordering = new CollectionOrdering(Seq(SortBy("c"), SortBy("a")))
      list.sorted(ordering) shouldBe List(c1x,c3x,c2x)
    }

    "sort descending two fields" - {
      val list: List[Value] = List(c1x,c2x,c3x)
      implicit val ordering = new CollectionOrdering(Seq(SortBy("c", descending=true), SortBy("a")))
      list.sorted shouldBe List(c2x,c1x,c3x)
    }
  }
}
