import eu.inn.binders.value.Text
import eu.inn.hyperstorage.api.{HyperStorageIndexSortItem, HyperStorageIndexSortOrder}
import eu.inn.hyperstorage.db._
import eu.inn.hyperstorage.indexing.{AstComparation, AstComparator, IndexLogic}
import eu.inn.parser.{HEval, HParser}
import eu.inn.parser.ast.{BinaryOperation, Expression, Identifier}
import org.scalatest.{FreeSpec, Matchers}

class FieldFiltersExtractor(sortByFields: Seq[HyperStorageIndexSortItem]) {
  final val sortByFieldsMap = sortByFields.zipWithIndex.map{
    case (s,index) ⇒ (Identifier.parse(s.fieldName), (s,index))
  }.toMap
  final val compareOps = Set(">", ">=", "<", "<=", "=").map(Identifier(_))
  final val andOp = Identifier.parse("and")

  def extract(expression: Expression): Seq[FieldFilter] = {
    expression match {
      case bop @ BinaryOperation(left, op, right) if compareOps.contains(op) ⇒
        val (identifier, opStr, constExp) =
          if (AstComparator.isConstantExpression(left) && right.isInstanceOf[Identifier]) {
            (right.asInstanceOf[Identifier], swapOp(op.segments.head), left)
          } else {
            (left.asInstanceOf[Identifier], op.segments.head, right)
          }

        // this is our identifier
        sortByFieldsMap.get(identifier).map { sf ⇒
          val tfieldName = IndexLogic.tableFieldName(sf._1, sf._2)
          val op = filterOp(opStr)
          val constVal = new HEval().eval(constExp)
          FieldFilter(tfieldName, constVal, op)
        }.toSeq

      case bop @ BinaryOperation(left, op, right) if op == andOp ⇒
        extract(left) ++ extract(right)

      case _ ⇒ Seq.empty
    }
  }

  def swapOp(op: String) = {
    op match {
      case ">" ⇒ "<"
      case ">=" ⇒ "<="
      case "<" ⇒ ">"
      case "<=" ⇒ ">="
      case "=" ⇒ "="
    }
  }

  def filterOp(op: String) = {
    op match {
      case ">" ⇒ FilterGt
      case ">=" ⇒ FilterGtEq
      case "<" ⇒ FilterLt
      case "<=" ⇒ FilterLtEq
      case "=" ⇒ FilterEq
    }
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

    "gt filter field with some other field" in {
      val expression = HParser(s""" id > "10" and x < 5 """).get
      val sortByFields =  Seq(HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)))
      val ff = new FieldFiltersExtractor(sortByFields).extract(expression)
      ff shouldBe Seq(FieldFilter("t0", Text("10"), FilterGt))
    }

    "gt filter field with or expression shouldn't match" in {
      val expression = HParser(s""" id > "10" or x < 5 """).get
      val sortByFields =  Seq(HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)))
      val ff = new FieldFiltersExtractor(sortByFields).extract(expression)
      ff shouldBe Seq.empty
    }
  }
}
