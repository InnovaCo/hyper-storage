package eu.inn.hyperstorage.db

import java.util.{Date, UUID}

import eu.inn.binders._
import eu.inn.binders.cassandra._
import eu.inn.binders.naming.CamelCaseToSnakeCaseConverter
import eu.inn.hyperstorage.CassandraConnector
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

trait ContentBase {
  def documentUri: String
  def revision: Long
  def transactionList: List[UUID]
}

case class Content(
                    documentUri: String,
                    itemSegment: String,
                    revision: Long,
                    transactionList: List[UUID],
                    body: Option[String],
                    isDeleted: Boolean,
                    createdAt: Date,
                    modifiedAt: Option[Date]
                  ) extends ContentBase

case class ContentStatic(
                    documentUri: String,
                    revision: Long,
                    transactionList: List[UUID]
                  ) extends ContentBase

case class Transaction(
                    dtQuantum: Long,
                    partition: Int,
                    documentUri: String,
                    uuid: UUID,
                    revision: Long,
                    body: String,
                    completedAt: Option[Date]
                  )

case class PendingIndex(
                       partition: Int,
                       documentUri: String,
                       indexId: String,
                       lastItemSegment: Option[String], // todo: rename
                       metaTransactionId: UUID
                       )

case class IndexMeta(
                      documentUri: String,
                      indexId: String,
                      status: Int,
                      sortBy: String,
                      filterBy: String,
                      tableName: String,
                      metaTransactionId: UUID
                    )

object IndexMeta {
  val STATUS_INDEXING = 0
  val STATUS_DELETING = 1
  val STATUS_NORMAL = 2
}

private [db] case class CheckPoint(lastQuantum: Long)

class Db(connector: CassandraConnector)(implicit ec: ExecutionContext) {
  private[this] lazy val session: com.datastax.driver.core.Session = connector.connect()
  private[this] lazy implicit val sessionQueryCache = new SessionQueryCache[CamelCaseToSnakeCaseConverter](session)
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

  def selectContent(documentUri: String, itemSegment: String): Future[Option[Content]] = cql"""
      select document_uri,item_segment,revision,transaction_list,body,is_deleted,created_at,modified_at from content
      where document_uri=$documentUri and item_segment=$itemSegment
    """.oneOption[Content]

  def selectContentCollection(documentUri: String, limit: Int): Future[Iterator[Content]] = cql"""
      select document_uri,item_segment,revision,transaction_list,body,is_deleted,created_at,modified_at from content
      where document_uri=$documentUri and item_segment > ''
      limit $limit
    """.all[Content]

  def selectContentCollectionFrom(documentUri: String, fromId: String, limit: Int): Future[Iterator[Content]] = cql"""
      select document_uri,item_segment,revision,transaction_list,body,is_deleted,created_at,modified_at from content
      where document_uri=$documentUri and item_segment > $fromId
      limit $limit
    """.all[Content]

  def selectContentCollectionDesc(documentUri: String, limit: Int): Future[Iterator[Content]] = cql"""
      select document_uri,item_segment,revision,transaction_list,body,is_deleted,created_at,modified_at from content
      where document_uri=$documentUri
      order by item_segment desc
      limit $limit
    """.all[Content]

  def selectContentCollectionDescFrom(documentUri: String, fromId: String, limit: Int): Future[Iterator[Content]] = cql"""
      select document_uri,item_segment,revision,transaction_list,body,is_deleted,created_at,modified_at from content
      where document_uri=$documentUri and item_segment < $fromId
      order by item_segment desc
      limit $limit
    """.all[Content]

  def selectContentStatic(documentUri: String): Future[Option[ContentStatic]] = cql"""
      select document_uri,revision,transaction_list from content
      where document_uri=$documentUri
      limit 1
    """.oneOption[ContentStatic]

  def insertContent(content: Content): Future[Unit] = cql"""
      insert into content(document_uri,item_segment,revision,transaction_list,body,is_deleted,created_at,modified_at)
      values(?,?,?,?,?,?,?,?)
    """.bind(content).execute()

  def selectTransaction(dtQuantum: Long, partition: Int, documentUri: String, uuid: UUID): Future[Option[Transaction]] = cql"""
      select dt_quantum,partition,document_uri,uuid,revision,body,completed_at from transaction
      where dt_quantum=$dtQuantum and partition=$partition and document_uri=$documentUri and uuid=$uuid
    """.oneOption[Transaction]

  def selectPartitionTransactions(dtQuantum: Long, partition: Int): Future[Iterator[Transaction]] = cql"""
      select dt_quantum,partition,document_uri,uuid,revision,body,completed_at from transaction
      where dt_quantum=$dtQuantum and partition=$partition
    """.all[Transaction]

  def insertTransaction(transaction: Transaction): Future[Unit] = cql"""
      insert into transaction(dt_quantum,partition,document_uri,uuid,revision,body,completed_at)
      values(?,?,?,?,?,?,?)
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
      select partition, document_uri, index_id, last_item_segment, meta_transaction_id
      from pending_indexes
      where partition=$partition
      limit $limit
    """.all[PendingIndex]

  def selectPendingIndex(partition: Int, documentId: String, indexId: String, metaTransactionId: UUID): Future[Option[PendingIndex]] = cql"""
      select partition, document_uri, index_id, last_item_segment, meta_transaction_id
      from pending_indexes
      where partition=$partition and document_id=$documentId and index_id=$indexId and meta_transaction_id=$metaTransactionId
    """.oneOption[PendingIndex]

  def deletePendingIndex(partition: Int, documentId: String, indexId: String, metaTransactionId: UUID) = cql"""
      delete
      from pending_indexes
      where partition=$partition and document_id=$documentId and index_id=$indexId and meta_transaction_id=$metaTransactionId
    """.execute()

  def selectIndexMeta(documentUri: String, indexId: String): Future[Option[IndexMeta]] = cql"""
      select document_uri, index_id, status, sort_by, filter_by, table_name, meta_transaction_id from index_meta
      where document_uri = $documentUri and index_id=$indexId
    """.oneOption[IndexMeta]

  def selectIndexMetas(documentUri: String): Future[Iterator[IndexMeta]] = cql"""
      select document_uri, index_id, status, sort_by, filter_by, table_name, meta_transaction_id from index_meta
      where document_uri = $documentUri
    """.all[IndexMeta]
}
