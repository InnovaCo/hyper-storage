package eu.inn.hyperstorage.workers.secondary

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import eu.inn.binders.value.{Null, Value}
import eu.inn.hyperbus.Hyperbus
import eu.inn.hyperstorage._
import eu.inn.hyperstorage.db._
import eu.inn.hyperstorage.indexing.{IndexDefTransaction, IndexLogic}
import eu.inn.hyperstorage.sharding.ShardTaskComplete
import eu.inn.hyperstorage.utils.FutureUtils
import eu.inn.metrics.MetricsTracker

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

@SerialVersionUID(1L) case class IndexContentTask(ttl: Long, indexDefTransaction: IndexDefTransaction, lastItemId: Option[String], processId: Long) extends SecondaryTaskTrait {
  def key = indexDefTransaction.documentUri
}

@SerialVersionUID(1L) case class IndexContentTaskResult(lastItemSegment: Option[String], processId: Long)

@SerialVersionUID(1L) case class IndexContentTaskFailed(processId: Long, reason: String) extends RuntimeException(s"Index content task for process $processId is failed with reason $reason")

trait IndexContentTaskWorker {
  def hyperbus: Hyperbus
  def db: Db
  def tracker: MetricsTracker
  def log: LoggingAdapter
  def indexManager: ActorRef
  implicit def executionContext: ExecutionContext

  def validateCollectionUri(uri: String)

  def indexNextBucket(task: IndexContentTask): Future[ShardTaskComplete] = {
    try {
      validateCollectionUri(task.key)
      // todo: cache indexDef
      db.selectIndexDef(task.indexDefTransaction.documentUri, task.indexDefTransaction.indexId) flatMap {
        case Some(indexDef) if indexDef.defTransactionId == task.indexDefTransaction.defTransactionId ⇒ indexDef.status match {
          case IndexDef.STATUS_INDEXING ⇒
            val bucketSize = 256 // todo: move to config, or make adaptive, or per index

            db.selectContentCollection(task.indexDefTransaction.documentUri, bucketSize, task.lastItemId.map((_, FilterGt))) flatMap { collectionItems ⇒
              FutureUtils.serial(collectionItems.toSeq) { item ⇒
                indexItem(indexDef, item)
              } flatMap { insertedItemIds ⇒

                if (insertedItemIds.isEmpty) {
                  // indexing is finished
                  // todo: fix code format
                  db.updateIndexDefStatus(task.indexDefTransaction.documentUri, task.indexDefTransaction.indexId, IndexDef.STATUS_NORMAL, task.indexDefTransaction.defTransactionId) flatMap { _ ⇒
                    db.deletePendingIndex(TransactionLogic.partitionFromUri(task.indexDefTransaction.documentUri), task.indexDefTransaction.documentUri, task.indexDefTransaction.indexId, task.indexDefTransaction.defTransactionId) map { _ ⇒
                      IndexContentTaskResult(None, task.processId)
                    }
                  }
                } else {
                  val last = insertedItemIds.last
                  db.updatePendingIndexLastItemId(TransactionLogic.partitionFromUri(task.indexDefTransaction.documentUri), task.indexDefTransaction.documentUri, task.indexDefTransaction.indexId, task.indexDefTransaction.defTransactionId, last) map { _ ⇒
                    IndexContentTaskResult(Some(last), task.processId)
                  }
                }
              }
            }


          case IndexDef.STATUS_NORMAL ⇒
            Future.successful(IndexContentTaskResult(None, task.processId))

          case IndexDef.STATUS_DELETING ⇒
            deleteIndexDefAndData(indexDef) map { _ ⇒
              IndexContentTaskResult(None, task.processId)
            }
        }

        case _ ⇒
          Future.failed(IndexContentTaskFailed(task.processId, s"Can't find index for ${task.indexDefTransaction}")) // todo: test this
      } map { r: IndexContentTaskResult ⇒
        ShardTaskComplete(task, r)
      }
    }
    catch {
      case NonFatal(e) ⇒
      Future.failed(e)
    }
  }

  def deleteIndexDefAndData(indexDef: IndexDef): Future[Unit] = {
    db.deleteIndex(indexDef.tableName, indexDef.documentUri, indexDef.indexId) flatMap { _ ⇒
      db.deleteIndexDef(indexDef.documentUri, indexDef.indexId) flatMap { _ ⇒
        db.deletePendingIndex(
          TransactionLogic.partitionFromUri(indexDef.documentUri), indexDef.documentUri, indexDef.indexId, indexDef.defTransactionId
        )
      }
    }
  }

  def indexItem(indexDef: IndexDef, item: Content): Future[String] = {
    import eu.inn.binders.json._
    if (log.isDebugEnabled) {
      log.debug(s"Indexing item $item with $indexDef")
    }

    // todo: cache this
    val contentValue = item.body.map { str ⇒
      str.parseJson[Value]
    } getOrElse {
      Null
    }

    // todo: cache this
    val sortBy = indexDef.sortBy.map { sortString ⇒
      val sortBy = IndexLogic.deserializeSortByFields(sortString)
      IndexLogic.extractSortFieldValues(sortBy, contentValue)
    } getOrElse {
      Seq.empty
    }

    val write: Boolean = indexDef.filterBy.map { filterBy ⇒
      IndexLogic.evaluateFilterExpression(filterBy, contentValue) recover {
        case NonFatal(e) ⇒
          if (log.isDebugEnabled) {
            log.debug(s"Can't evaluate expression: `$filterBy` for $item", e)
          }
          false
      } get
    } getOrElse {
      true
    }

    if (write) {
      val indexContent = IndexContent(
        item.documentUri, indexDef.indexId, item.itemId, item.revision, item.body, item.createdAt, item.modifiedAt
      )
      db.insertIndexItem(indexDef.tableName, sortBy, indexContent) map { _ ⇒
        item.itemId
      }
    }
    else {
      Future.successful(item.itemId)
    }
  }
}
