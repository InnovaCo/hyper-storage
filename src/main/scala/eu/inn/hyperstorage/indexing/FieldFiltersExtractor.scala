package eu.inn.hyperstorage.indexing

import eu.inn.hyperstorage.api.HyperStorageIndexSortItem
import eu.inn.hyperstorage.db._
import eu.inn.parser.ast.{BinaryOperation, Expression, Identifier}
import eu.inn.parser.{HEval, HParser}

class FieldFiltersExtractor(sortByFields: Seq[HyperStorageIndexSortItem]) {
  private final val size = sortByFields.size
  private final val sortByFieldsMap = sortByFields.zipWithIndex.map{
    case (s,index) ⇒ (parseIdentifier(s.fieldName), (s,index,
        IndexLogic.tableFieldName(s, size, index)
      ))
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
        sf._3,
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
