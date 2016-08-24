package eu.inn.hyperstorage.indexing

import eu.inn.binders.value.{Null, Obj, Value}
import eu.inn.parser.ast.Identifier
import eu.inn.parser.eval.{EvalIdentifierNotFound, ValueContext}
import eu.inn.parser.{HEval, HParser}
import eu.inn.hyperstorage.api._

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

  def deserializeSortByFields(sortBy: String): Seq[HyperStorageIndexSortItem] = {
    import eu.inn.binders.json._
    sortBy.parseJson[Seq[HyperStorageIndexSortItem]]
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

    sortBy.zipWithIndex.map { case (sortItem, index) ⇒
      val fieldName = tableFieldType(sortItem) + index.toString
      val fieldValue = HParser(sortItem.fieldName) match {
        case Success(identifier: Identifier) if valueContext.identifier.isDefinedAt(identifier) ⇒
          valueContext.identifier(identifier)
        case _ ⇒ Null
      }
      (fieldName, fieldValue)
    }
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
}
