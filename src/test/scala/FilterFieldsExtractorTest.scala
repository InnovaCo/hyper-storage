import eu.inn.binders.value.{Number, Text}
import eu.inn.hyperstorage.api.{HyperStorageIndexSortFieldType, HyperStorageIndexSortItem, HyperStorageIndexSortOrder}
import eu.inn.hyperstorage.db._
import eu.inn.hyperstorage.indexing.{AstComparation, AstComparator, IndexLogic}
import eu.inn.parser.{HEval, HParser}
import eu.inn.parser.ast.{BinaryOperation, Expression, Identifier}
import org.scalatest.{FreeSpec, Matchers}

class FieldFiltersExtractor(sortByFields: Seq[HyperStorageIndexSortItem]) {
  final val sortByFieldsMap = sortByFields.zipWithIndex.map{
    case (s,index) ⇒ (parseIdentifier(s.fieldName), (s,index))
  }.toMap
  final val compareOps = Set(">", ">=", "<", "<=", "=").map(Identifier(_))
  final val andOp = parseIdentifier("and")

  def extract(expression: Expression): Seq[FieldFilter] = {
    expression match {
      case bop @ BinaryOperation(left: Identifier, op, right) if compareOps.contains(op) && AstComparator.isConstantExpression(right) ⇒
        fieldFilterSeq(left, op.segments.head, right)

      case bop @ BinaryOperation(left, op, right: Identifier) if compareOps.contains(op) && AstComparator.isConstantExpression(left) ⇒
        fieldFilterSeq(right, swapOp(op.segments.head), left)

      case bop @ BinaryOperation(left, op, right) if op == andOp ⇒
        extract(left) ++ extract(right)

      case _ ⇒ Seq.empty
    }
  }

  private def fieldFilterSeq(varIdent: Identifier, op: String, constExpr: Expression): Seq[FieldFilter] = {
    sortByFieldsMap.get(varIdent).map { sf ⇒
      FieldFilter(
        IndexLogic.tableFieldName(sf._1, sf._2),
        new HEval().eval(constExpr),
        filterOp(op)
      )
    }.toSeq
  }

  private def swapOp(op: String) = {
    op match {
      case ">" ⇒ "<"
      case ">=" ⇒ "<="
      case "<" ⇒ ">"
      case "<=" ⇒ ">="
      case "=" ⇒ "="
    }
  }

  private def filterOp(op: String) = {
    op match {
      case ">" ⇒ FilterGt
      case ">=" ⇒ FilterGtEq
      case "<" ⇒ FilterLt
      case "<=" ⇒ FilterLtEq
      case "=" ⇒ FilterEq
    }
  }

  private def parseIdentifier(s: String): Identifier = {
    new HParser(s).Ident.run().get
  }
}

class FilterFieldsExtractorTest extends FreeSpec with Matchers {
  "FilterFieldsExtractor" - {
    "single gt filter field" in {
      val expression = HParser(s""" id > "10" """).get
      val sortByFields =  Seq(HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)))
      val ff = new FieldFiltersExtractor(sortByFields).extract(expression)
      ff shouldBe Seq(FieldFilter("t0", Text("10"), FilterGt))
    }

    "single lt filter field" in {
      val expression = HParser(s""" id < "10" """).get
      val sortByFields =  Seq(HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)))
      val ff = new FieldFiltersExtractor(sortByFields).extract(expression)
      ff shouldBe Seq(FieldFilter("t0", Text("10"), FilterLt))
    }

    "single gteq filter field" in {
      val expression = HParser(s""" id >= "10" """).get
      val sortByFields =  Seq(HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)))
      val ff = new FieldFiltersExtractor(sortByFields).extract(expression)
      ff shouldBe Seq(FieldFilter("t0", Text("10"), FilterGtEq))
    }

    "single lteq filter field" in {
      val expression = HParser(s""" id <= "10" """).get
      val sortByFields =  Seq(HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)))
      val ff = new FieldFiltersExtractor(sortByFields).extract(expression)
      ff shouldBe Seq(FieldFilter("t0", Text("10"), FilterLtEq))
    }

    "single eq filter field" in {
      val expression = HParser(s""" id = "10" """).get
      val sortByFields =  Seq(HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)))
      val ff = new FieldFiltersExtractor(sortByFields).extract(expression)
      ff shouldBe Seq(FieldFilter("t0", Text("10"), FilterEq))
    }

    "single gt reversed filter field" in {
      val expression = HParser(s""" "10" < id """).get
      val sortByFields =  Seq(HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)))
      val ff = new FieldFiltersExtractor(sortByFields).extract(expression)
      ff shouldBe Seq(FieldFilter("t0", Text("10"), FilterGt))
    }

    "gt filter field with some other field" in {
      val expression = HParser(s""" id > "10" and x < 5 """).get
      val sortByFields =  Seq(HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)))
      val ff = new FieldFiltersExtractor(sortByFields).extract(expression)
      ff shouldBe Seq(FieldFilter("t0", Text("10"), FilterGt))
    }

    "eq filter field with some other fields" in {
      val expression = HParser(s""" id = "10" and x < 5 and z*3 > 24""").get
      val sortByFields =  Seq(HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)))
      val ff = new FieldFiltersExtractor(sortByFields).extract(expression)
      ff shouldBe Seq(FieldFilter("t0", Text("10"), FilterEq))
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
