package eu.inn.hyperstorage.indexing

import eu.inn.binders.value.{Null, Obj, Value}
import eu.inn.hyperbus.model.utils.SortBy
import eu.inn.parser.ast.{Expression, Identifier}
import eu.inn.parser.eval.{EvalIdentifierNotFound, ValueContext}
import eu.inn.parser.{HEval, HParser}
import eu.inn.hyperstorage.api._
import eu.inn.hyperstorage.db._

import scala.util.{Success, Try}

object IndexLogic {
  def tableName(sortBy: Seq[HyperStorageIndexSortItem]): String = {
    if (sortBy.isEmpty)
      "index_content"
    else {
      sortBy.zipWithIndex.foldLeft(new StringBuilder("index_content_")) { case (tableName, (sortItem, index)) ⇒
        HParser(sortItem.fieldName) match {
          case Success(Identifier(seq)) ⇒
          case _ ⇒ throw new IllegalArgumentException(s"Index field name is invalid: ${sortItem.fieldName}")
        }

        tableName
          .append(tableFieldType(sortItem))
          .append(sortItem.order match {
            case Some("desc") ⇒ "d"
            case _ ⇒ "a"
          })
          .append(index)
      }.toString
    }
  }

  private def tableFieldType(sortItem: HyperStorageIndexSortItem): String = {
    sortItem.fieldType match {
      case Some("decimal") ⇒ "d"
      case _ ⇒ "t"
    }
  }

  def serializeSortByFields(sortBy: Seq[HyperStorageIndexSortItem]): Option[String] = {
    import eu.inn.binders.json._
    if (sortBy.nonEmpty) Some(sortBy.toJson) else None
  }

  def extractSortFieldValues(sortBy: Seq[HyperStorageIndexSortItem], value: Value): Seq[(String, Value)] = {
    val valueContext = value match {
      case obj: Obj ⇒ ValueContext(obj)
      case _ ⇒ ValueContext(Obj.empty)
    }
    val size = sortBy.size
    sortBy.zipWithIndex.map { case (sortItem, index) ⇒
      val fieldName = tableFieldName(sortItem, size, index)
      val fieldValue = HParser(sortItem.fieldName) match {
        case Success(identifier: Identifier) if valueContext.identifier.isDefinedAt(identifier) ⇒
          valueContext.identifier(identifier)
        case _ ⇒ Null
      }
      (fieldName, fieldValue)
    }
  }

  def tableFieldName(sortItem: HyperStorageIndexSortItem, sortItemSize: Int, index: Int) = {
    if(index == (sortItemSize-1) && sortItem.fieldName == "id")
      "item_id"
    else
      tableFieldType(sortItem) + index.toString
  }

  def validateFilterExpression(expression: String): Try[Boolean] = {
    Try {
      HEval(expression) // we evaluate with empty context, to check everything except EvalIdentifierNotFound
      true
    } recover {
      case e: EvalIdentifierNotFound ⇒
        true
    }
  }

  def evaluateFilterExpression(expression: String, value: Value): Try[Boolean] = {
    val v = value match {
      case o: Obj ⇒ o
      case _ ⇒ Obj.empty
    }
    HEval(expression, v).map(_.asBoolean)
  }

  def weighIndex(queryExpression: Option[Expression], querySortOrder: Seq[SortBy],
                 indexFilterExpression: Option[Expression], indexSortOrder: Seq[HyperStorageIndexSortItem]): Int = {

    val filterWeight = (queryExpression, indexFilterExpression) match {
      case (None, Some(_)) ⇒ -1000000
      case (Some(_), None) ⇒ -30
      case (None, None) ⇒ 0
      case (Some(q), Some(i)) ⇒
        AstComparator.compare(i,q) match {
          case AstComparation.Equal ⇒ 20
          case AstComparation.Wider ⇒ 10
          case AstComparation.NotEqual ⇒ -1000001
        }
    }

    val orderWeight = OrderFieldsLogic.weighOrdering(querySortOrder, indexSortOrder)
    orderWeight + filterWeight
  }

  def leastRowsFilterFields(indexSortedBy: Seq[HyperStorageIndexSortItem],
                            queryFilterFields: Seq[FieldFilter],
                            prevFilterFieldsSize: Int,
                            prevFilterReachedEnd: Boolean,
                            value: Obj,
                            reversed: Boolean): Seq[FieldFilter] = {

    val valueContext = ValueContext(value)
    val size = indexSortedBy.size
    val isbIdx = indexSortedBy.zipWithIndex.map {
      case (sortItem, index) ⇒
        val fieldName = tableFieldName(sortItem, size, index)
        val fieldValue = HParser(sortItem.fieldName) match {
          case Success(identifier: Identifier) if valueContext.identifier.isDefinedAt(identifier) ⇒
            valueContext.identifier(identifier)
          case _ ⇒ Null
        }
        (fieldName, fieldValue, sortItem.order.forall(_ == HyperStorageIndexSortOrder.ASC), index, sortItem.fieldType.getOrElse(HyperStorageIndexSortFieldType.TEXT))
    }

    val reachedEnd = !queryFilterFields.forall { q ⇒
      if (q.op != FilterEq) {
        isbIdx.find(_._1 == q.name).map { i ⇒
          //val op = if (reversed) swapOp(q.op) else q.op
          valueRangeMatches(i._2, q.value, q.op, i._5)
        } getOrElse {
          true
        }
      } else {
        true
      }
    }

    if (reachedEnd) Seq.empty else {
      val startIndex = isbIdx.lastIndexWhere(isb ⇒ queryFilterFields.exists(qf ⇒ qf.name == isb._1 && qf.op == FilterEq)) + 1
      val lastIndex = if (prevFilterFieldsSize == 0 || !prevFilterReachedEnd) {
        size - 1
      } else {
        prevFilterFieldsSize - 2
      }

      isbIdx.flatMap {
        case (fieldName, fieldValue, fieldAscending, index, _) if index >= startIndex ⇒
          if (index == lastIndex) {
            val op = if (reversed ^ fieldAscending)
              FilterGt
            else
              FilterLt
            Some(FieldFilter(fieldName, fieldValue, op))
          } else if (index <= lastIndex) {
            Some(FieldFilter(fieldName, fieldValue, FilterEq))
          } else {
            None
          }
        case _ ⇒ None
      }
    }
  }

  def valueRangeMatches(a: Value, b: Value, op: FilterOperator, sortFieldType: String): Boolean = {
    op match {
      case FilterGt ⇒ greater(a,b,sortFieldType)
      case FilterGtEq ⇒ a == b || greater(a,b,sortFieldType)
      case FilterLt ⇒ greater(b,a,sortFieldType)
      case FilterLtEq ⇒ a == b || greater(b,a,sortFieldType)
      case FilterEq ⇒ a == b
    }
  }

  def greater(a: Value, b: Value, sortFieldType: String): Boolean = {
    sortFieldType match {
      case HyperStorageIndexSortFieldType.DECIMAL ⇒ a.asBigDecimal > b.asBigDecimal
      case _ => a.asString > b.asString
    }
  }

  def mergeLeastQueryFilterFields(queryFilterFields: Seq[FieldFilter],leastFilterFields: Seq[FieldFilter]): Seq[FieldFilter] = {
    if (leastFilterFields.isEmpty) {
      queryFilterFields
    }
    else {
      queryFilterFields.filter(_.op == FilterEq) ++ leastFilterFields
    }
  }

  private def swapOp(op: FilterOperator) = {
    op match {
      case FilterGt ⇒ FilterLt
      case FilterGtEq ⇒ FilterLtEq
      case FilterLt ⇒ FilterGt
      case FilterLtEq ⇒ FilterGtEq
      case FilterEq ⇒ FilterEq
    }
  }
}
