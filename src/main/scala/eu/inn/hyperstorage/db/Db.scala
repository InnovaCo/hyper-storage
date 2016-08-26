package eu.inn.hyperstorage.db

import java.util.{Date, UUID}

import eu.inn.binders._
import eu.inn.binders.cassandra._
import eu.inn.binders.naming.CamelCaseToSnakeCaseConverter
import eu.inn.binders.value.{Number, Text, Value}
import eu.inn.hyperstorage.CassandraConnector
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

trait ContentBase {
  def documentUri: String

  def revision: Long

  def transactionList: List[UUID]

  def isDeleted: Boolean
}

trait CollectionContent {
  def documentUri: String
  def itemId: String
  def revision: Long
  def body: Option[String]
  def createdAt: Date
  def modifiedAt: Option[Date]
}

case class Content(
                    documentUri: String,
                    itemId: String,
                    revision: Long,
                    transactionList: List[UUID],
                    body: Option[String],
                    isDeleted: Boolean,
                    createdAt: Date,
                    modifiedAt: Option[Date]
                  ) extends ContentBase with CollectionContent

case class ContentStatic(
                          documentUri: String,
                          revision: Long,
                          transactionList: List[UUID],
                          isDeleted: Boolean
                        ) extends ContentBase

case class Transaction(
                        dtQuantum: Long,
                        partition: Int,
                        documentUri: String,
                        itemId: String,
                        uuid: UUID,
                        revision: Long,
                        body: String,
                        completedAt: Option[Date]
                      )

case class PendingIndex(
                         partition: Int,
                         documentUri: String,
                         indexId: String,
                         lastItemId: Option[String],
                         defTransactionId: UUID
                       )

case class IndexDef(
                     documentUri: String,
                     indexId: String,
                     status: Int,
                     sortBy: Option[String],
                     filterBy: Option[String],
                     tableName: String,
                     defTransactionId: UUID
                   )

case class IndexContent(
                         documentUri: String,
                         indexId: String,
                         itemId: String,
                         revision: Long,
                         body: Option[String],
                         createdAt: Date,
                         modifiedAt: Option[Date]
                       ) extends CollectionContent

object IndexDef {
  val STATUS_INDEXING = 0
  val STATUS_DELETING = 1
  val STATUS_NORMAL = 2
}

sealed trait FilterOperator
case object FilterEq extends FilterOperator
case object FilterGt extends FilterOperator
case object FilterGtEq extends FilterOperator
case object FilterLt extends FilterOperator
case object FilterLtEq extends FilterOperator
case class CkField(name: String, ascending: Boolean)
case class FieldFilter(name: String, value: Value, op: FilterOperator)

private[db] case class CheckPoint(lastQuantum: Long)

class Db(connector: CassandraConnector)(implicit ec: ExecutionContext) {
  private[this] lazy val session: com.datastax.driver.core.Session = connector.connect()
  private[this] lazy implicit val sessionQueryCache = new GuavaSessionQueryCache[CamelCaseToSnakeCaseConverter](session)
  val log = LoggerFactory.getLogger(getClass)

  def preStart(): Unit = {
    session
  }

  def close() = {
    try {
      val cluster = session.getCluster
      session.close()
      cluster.close()
    }
    catch {
      case NonFatal(e) ⇒
        log.error(s"Can't close C* session", e)
    }
  }

  def selectContent(documentUri: String, itemId: String): Future[Option[Content]] = cql"""
      select document_uri,item_id,revision,transaction_list,is_deleted,body,created_at,modified_at from content
      where document_uri=$documentUri and item_id=$itemId
    """.oneOption[Content]

  def selectContentCollection(documentUri: String, limit: Int, fromId: Option[String], ascending: Boolean = true): Future[Iterator[Content]] = {
    val orderClause = if(ascending) {
      Dynamic("order by item_id asc")
    }
    else {
      Dynamic("order by item_id desc")
    }
    val itemIdFilter = fromId.map { id ⇒
      if(ascending) {
        Dynamic(" and item_id > ?")
      }
      else {
        Dynamic(" and item_id < ?")
      }
    } getOrElse {Dynamic("")}
    val c = cql"""
      select document_uri,item_id,revision,transaction_list,is_deleted,body,created_at,modified_at from content
      where document_uri=? $itemIdFilter
      $orderClause
      limit ?
    """
    if (fromId.isDefined) {
      c.bindArgs(documentUri, fromId.get, limit)
    }
    else {
      c.bindArgs(documentUri, limit)
    }
    c.stmt.execute.map(rows => rows.iterator.flatMap{it ⇒
      if(it.row.isNull("item_id")) None else Some(it.unbind[Content])
    })
  }

  def selectContentStatic(documentUri: String): Future[Option[ContentStatic]] = cql"""
      select document_uri,revision,transaction_list,is_deleted from content
      where document_uri=$documentUri
      limit 1
    """.oneOption[ContentStatic]

  def insertContent(content: Content): Future[Unit] = cql"""
      insert into content(document_uri,item_id,revision,transaction_list,is_deleted,body,created_at,modified_at)
      values(?,?,?,?,?,?,?,?)
    """.bind(content).execute()

  def deleteContentItem(content: ContentBase, itemId: String): Future[Unit] = cql"""
      begin batch
        update content
        set transaction_list = ${content.transactionList}, revision = ${content.revision}
        where document_uri = ${content.documentUri};
        delete from content
        where document_uri = ${content.documentUri} and item_id = $itemId;
      apply batch;
    """.execute()

  def selectTransaction(dtQuantum: Long, partition: Int, documentUri: String, uuid: UUID): Future[Option[Transaction]] = cql"""
      select dt_quantum,partition,document_uri,item_id,uuid,revision,body,completed_at from transaction
      where dt_quantum=$dtQuantum and partition=$partition and document_uri=$documentUri and uuid=$uuid
    """.oneOption[Transaction]

  def selectPartitionTransactions(dtQuantum: Long, partition: Int): Future[Iterator[Transaction]] = cql"""
      select dt_quantum,partition,document_uri,item_id,uuid,revision,body,completed_at from transaction
      where dt_quantum=$dtQuantum and partition=$partition
    """.all[Transaction]

  def insertTransaction(transaction: Transaction): Future[Unit] = cql"""
      insert into transaction(dt_quantum,partition,document_uri,item_id,uuid,revision,body,completed_at)
      values(?,?,?,?,?,?,?,?)
    """.bind(transaction).execute()

  def completeTransaction(transaction: Transaction): Future[Unit] = cql"""
      update transaction set completed_at=dateOf(now())
      where dt_quantum=${transaction.dtQuantum}
        and partition=${transaction.partition}
        and document_uri=${transaction.documentUri}
        and uuid=${transaction.uuid}
    """.execute()

  def deleteTransaction(transaction: Transaction): Future[Unit] = cql"""
      delete transaction
      where dt_quantum=${transaction.dtQuantum}
        and partition=${transaction.partition}
        and document_uri=${transaction.documentUri}
        and uuid=${transaction.uuid}
    """.execute()

  def removeCompleteTransactionsFromList(documentUri: String, transactions: List[UUID]) = cql"""
      update content
        set transaction_list = transaction_list - $transactions
      where document_uri = $documentUri
    """.execute()

  def selectCheckpoint(partition: Int): Future[Option[Long]] = cql"""
      select last_quantum from checkpoint where partition = $partition
    """.oneOption[CheckPoint].map(_.map(_.lastQuantum))

  def updateCheckpoint(partition: Int, lastQuantum: Long): Future[Unit] = cql"""
      insert into checkpoint(partition, last_quantum) values($partition, $lastQuantum)
    """.execute()

  def selectPendingIndexes(partition: Int, limit: Int): Future[Iterator[PendingIndex]] = cql"""
      select partition, document_uri, index_id, last_item_id, def_transaction_id
      from pending_index
      where partition=$partition
      limit $limit
    """.all[PendingIndex]

  def selectPendingIndex(partition: Int, documentId: String, indexId: String, defTransactionId: UUID): Future[Option[PendingIndex]] = cql"""
      select partition, document_uri, index_id, last_item_id, def_transaction_id
      from pending_index
      where partition=$partition and document_uri=$documentId and index_id=$indexId and def_transaction_id=$defTransactionId
    """.oneOption[PendingIndex]

  def deletePendingIndex(partition: Int, documentId: String, indexId: String, defTransactionId: UUID) = cql"""
      delete
      from pending_index
      where partition=$partition and document_uri=$documentId and index_id=$indexId and def_transaction_id=$defTransactionId
    """.execute()

  def updatePendingIndexLastItemId(partition: Int, documentId: String, indexId: String, defTransactionId: UUID, lastItemId: String) = cql"""
      update pending_index
      set last_item_id = $lastItemId
      where partition=$partition and document_uri=$documentId and index_id=$indexId and def_transaction_id=$defTransactionId
    """.execute()

  def insertPendingIndex(pendingIndex: PendingIndex): Future[Unit] = cql"""
      insert into pending_index(partition, document_uri, index_id, last_item_id, def_transaction_id)
      values (?,?,?,?,?)
    """.bind(pendingIndex).execute()

  def selectIndexDef(documentUri: String, indexId: String): Future[Option[IndexDef]] = cql"""
      select document_uri, index_id, status, sort_by, filter_by, table_name, def_transaction_id
      from index_def
      where document_uri = $documentUri and index_id=$indexId
    """.oneOption[IndexDef]

  def selectIndexDefs(documentUri: String): Future[Iterator[IndexDef]] = cql"""
      select document_uri, index_id, status, sort_by, filter_by, table_name, def_transaction_id
      from index_def
      where document_uri = $documentUri
    """.all[IndexDef]

  def insertIndexDef(indexDef: IndexDef): Future[Unit] = cql"""
      insert into index_def(document_uri, index_id, status, sort_by, filter_by, table_name, def_transaction_id)
      values (?,?,?,?,?,?,?)
    """.bind(indexDef).execute()

  def updateIndexDefStatus(documentUri: String, indexId: String, newStatus: Int, defTransactionId: UUID): Future[Unit] = cql"""
      update index_def
      set status = $newStatus, def_transaction_id = $defTransactionId
      where document_uri = $documentUri and index_id = $indexId
    """.execute()

  def deleteIndexDef(documentUri: String, indexId: String): Future[Unit] = cql"""
      delete from index_def
      where document_uri = $documentUri and index_id = $indexId
    """.execute()

  def insertIndexItem(indexTable: String, sortFields: Seq[(String, Value)], indexContent: IndexContent): Future[Unit] = {
    val tableName = Dynamic(indexTable)
    val sortFieldNames = if (sortFields.isEmpty) Dynamic("") else Dynamic(sortFields.map(_._1).mkString(",", ",", ""))
    val sortFieldPlaces = if (sortFields.isEmpty) Dynamic("") else Dynamic(sortFields.map(_ ⇒ "?").mkString(",", ",", ""))
    val cql = cql"""
      insert into $tableName(document_uri,index_id,item_id,revision,body,created_at,modified_at$sortFieldNames)
      values(?,?,?,?,?,?,?$sortFieldPlaces)
    """.bindPartial(indexContent)

    sortFields.foreach {
      case (name, Text(s)) ⇒ cql.boundStatement.setString(name, s)
      case (name, Number(n)) ⇒ cql.boundStatement.setDecimal(name, n.bigDecimal)
      case (name, v) ⇒ throw new IllegalArgumentException(s"Can't bind $name value $v") // todo: do something
    }
    cql.execute()
  }

  def selectIndexCollection(indexTable: String, documentUri: String, indexId: String,
                            filter: Seq[FieldFilter],
                            orderByFields: Seq[CkField],
                            limit: Int): Future[Iterator[IndexContent]] = {

    val tableName = Dynamic(indexTable)
    val filterEqualFields = if (filter.isEmpty)
      Dynamic("")
    else
      Dynamic {
        filter.map {
          case FieldFilter(name, _, FilterEq) ⇒ s"$name = ?"
          case FieldFilter(name, _, FilterGt) ⇒ s"$name > ?"
          case FieldFilter(name, _, FilterGtEq) ⇒ s"$name >= ?"
          case FieldFilter(name, _, FilterLt) ⇒ s"$name < ?"
          case FieldFilter(name, _, FilterLtEq) ⇒ s"$name <= ?"
        } mkString("and ", " and ", "")
      }

    val orderByDynamic = if (orderByFields.isEmpty)
      Dynamic("")
    else
      Dynamic(orderByFields.map {
        case CkField(name, true) ⇒ s"$name asc"
        case CkField(name, false) ⇒ s"$name desc"
      } mkString ("order by ", ",", ""))

    val c = cql"""
      select document_uri,index_id,item_id,revision,body,created_at,modified_at from $tableName
      where document_uri=? and index_id=? $filterEqualFields
      $orderByDynamic
      limit ?
    """

    c.bindArgs(documentUri, indexId)
    filter foreach {
      case FieldFilter(name, Text(s), _) ⇒ c.bindArgs(s)
      case FieldFilter(name, Number(n), _) ⇒ c.bindArgs(n)
      case FieldFilter(name, other, _) ⇒ throw new IllegalArgumentException(s"Can't bind $name value $other") // todo: do something
    }

    c.bindArgs(limit)
    c.all[IndexContent]
  }

  def deleteIndexItem(indexTable: String, documentUri: String, indexId: String, itemId: String): Future[Unit] = {
    val tableName = Dynamic(indexTable)
    cql"""
      delete from $tableName
      where document_uri=$documentUri and index_id=$indexId and item_id = $itemId
    """.execute()
  }

  def deleteIndex(indexTable: String, documentUri: String, indexId: String): Future[Unit] = {
    val tableName = Dynamic(indexTable)
    cql"""
      delete from $tableName
      where document_uri = $documentUri and index_id=$indexId
    """.execute()
  }
}
