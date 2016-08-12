package eu.inn.parser

import eu.inn.binders.{value ⇒ bn}
import eu.inn.parser.ast.{AstComparation, AstComparator}
import org.scalatest.{FreeSpec, Matchers}

class AstComparationTest extends FreeSpec with Matchers {
  import AstComparation._

  "AstComparation" - {
    "Expression can be equal" in {
      AstComparator.compare(HParser("x + 5").get, HParser("x + 5").get) shouldBe Equal
      AstComparator.compare(HParser("x > 4").get, HParser("x > 4").get) shouldBe Equal
    }

    "Expression can be not equal" in {
      AstComparator.compare(HParser("x + y").get, HParser("x + 5").get) shouldBe NotEqual
      AstComparator.compare(HParser("x > 4").get, HParser("x > 3").get) shouldBe NotEqual
    }

    "Expression can be wider for `> <`" in {
      AstComparator.compare(HParser("x > 4").get, HParser("x > 5").get) shouldBe Wider
      AstComparator.compare(HParser("x >= 4").get, HParser("x >= 5").get) shouldBe Wider
      AstComparator.compare(HParser("x < 3").get, HParser("x < 2").get) shouldBe Wider
      AstComparator.compare(HParser("x <= 4").get, HParser("x <= 1").get) shouldBe Wider
      AstComparator.compare(HParser("x*4 <= 4").get, HParser("x*4 <= 1").get) shouldBe Wider
    }

    "Expression can be wider for `has / not`" in {
      AstComparator.compare(HParser("x has [1,2,3]").get, HParser("x has [1,2]").get) shouldBe Wider
      AstComparator.compare(HParser("x has not [1,2,3]").get, HParser("x has not [1,2,3,4]").get) shouldBe Wider
    }

    "Expression can be wider for `or`" in {
      AstComparator.compare(HParser("x > 5 or y < 2").get, HParser("x > 5").get) shouldBe Wider
      AstComparator.compare(HParser("x has [5,1,3,4] or y < 2").get, HParser("x has [5,1,3]").get) shouldBe Wider
      AstComparator.compare(HParser("x or y < 2").get, HParser("y < 2").get) shouldBe Wider
      AstComparator.compare(HParser("x or y").get, HParser("y").get) shouldBe Wider
    }

    "Expression can be wider for `and`" in {
      AstComparator.compare(HParser("x").get, HParser("x and y = 20").get) shouldBe Wider
      AstComparator.compare(HParser("x > 5").get, HParser("x > 5 and y = 20").get) shouldBe Wider
      AstComparator.compare(HParser("x").get, HParser("x and y").get) shouldBe Wider
      AstComparator.compare(HParser("x").get, HParser("x+1 and y").get) shouldBe NotEqual
    }
  }
}
