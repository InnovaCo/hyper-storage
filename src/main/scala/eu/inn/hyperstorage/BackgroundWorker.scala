package eu.inn.hyperstorage

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Status}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import com.datastax.driver.core.utils.UUIDs
import eu.inn.binders.value.{Null, Value}
import eu.inn.hyperbus.model._
import eu.inn.hyperbus.{Hyperbus, IdGenerator}
import eu.inn.hyperstorage.db._
import eu.inn.hyperstorage.api._
import eu.inn.hyperstorage.indexing.{IndexDefTransaction, IndexLogic, IndexManager}
import eu.inn.hyperstorage.metrics.Metrics
import eu.inn.hyperstorage.sharding.{ShardTask, ShardTaskComplete}
import eu.inn.hyperstorage.utils.FutureUtils
import eu.inn.metrics.MetricsTracker

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.{Success, Try}

// todo: do we really need a ShardTaskComplete ?
// todo: use strictly Hyperbus OR akka serialization for the internal akka-cluster

trait BackgroundTaskTrait extends ShardTask {
  def ttl: Long

  def isExpired = ttl < System.currentTimeMillis()

  def group = "hyper-storage-background-worker"
}

@SerialVersionUID(1L) case class BackgroundTask(ttl: Long, documentUri: String) extends BackgroundTaskTrait {
  def key = documentUri
}

@SerialVersionUID(1L) case class BackgroundTaskResult(documentUri: String, transactions: Seq[UUID])

// todo: request -> string as for the Foreground (strictly Hyperbus OR akka serialization)
@SerialVersionUID(1L) case class IndexDefTask(ttl: Long, request: Request[Body]) extends BackgroundTaskTrait {
  def key: String = request match {
    case post: HyperStorageIndexPost ⇒ post.path
    case delete: HyperStorageIndexDelete ⇒ delete.path
  }
}

@SerialVersionUID(1L) case class IndexNextBucketTask(ttl: Long, indexDefTransaction: IndexDefTransaction, lastItemId: Option[String], processId: Long) extends BackgroundTaskTrait {
  def key = indexDefTransaction.documentUri
}

@SerialVersionUID(1L) case class IndexContentTaskResult(lastItemSegment: Option[String], processId: Long)

@SerialVersionUID(1L) case class IndexContentTaskFailed(processId: Long, reason: String) extends RuntimeException(s"Index content task for process $processId is failed with reason $reason")

@SerialVersionUID(1L) case class NoSuchResourceException(documentUri: String) extends RuntimeException(s"No such resource: $documentUri")

@SerialVersionUID(1L) case class IncorrectDataException(documentUri: String, reason: String) extends RuntimeException(s"Data for $documentUri is incorrect: $reason")

@SerialVersionUID(1L) case class BackgroundTaskFailedException(documentUri: String, reason: String) extends RuntimeException(s"Background task for $documentUri is failed: $reason")

class BackgroundWorker(hyperbus: Hyperbus, db: Db, tracker: MetricsTracker, indexManager: ActorRef) extends Actor with ActorLogging {

  import ContentLogic._
  import context._

  override def receive: Receive = {
    case task: BackgroundTask ⇒
      executeBackgroundTask(sender(), task)

    case task: IndexDefTask ⇒
      executeIndexDefTask(sender(), task)

    case task: IndexNextBucketTask ⇒
      indexNextBucket(sender(), task)
  }

  def executeBackgroundTask(owner: ActorRef, task: BackgroundTask): Unit = {
    val ResourcePath(documentUri, itemId) = ContentLogic.splitPath(task.documentUri)
    if (!itemId.isEmpty) {
      // todo: is this have to be a success?
      owner ! Status.Success {
        val e = new IllegalArgumentException(s"Background task key ${task.key} doesn't correspond to $documentUri")
        ShardTaskComplete(task, e)
      }
    }
    else {
      tracker.timeOfFuture(Metrics.BACKGROUND_PROCESS_TIME) {
        db.selectContentStatic(task.documentUri) flatMap {
          case None ⇒
            log.error(s"Didn't found resource to background complete, dismissing task: $task")
            Future(ShardTaskComplete(task, NoSuchResourceException(task.documentUri)))
          case Some(content) ⇒
            try {
              if (log.isDebugEnabled) {
                log.debug(s"Background task for $content")
              }
              completeTransactions(task, content)
            } catch {
              case NonFatal(e) ⇒
                log.error(e, s"Background task $task didn't complete")
                Future(ShardTaskComplete(task, e))
            }
        }
      } pipeTo owner
    }
  }

  def updateIndexes(content: ContentStatic, incompleteTransactions: Seq[UnwrappedTransaction]): Future[Unit] = {
    if (ContentLogic.isCollectionUri(content.documentUri)) {
      val isCollectionDelete = incompleteTransactions.exists { it ⇒
        it.transaction.itemId.isEmpty && it.unwrappedBody.method == Method.FEED_DELETE
      }
      if (isCollectionDelete) {
        // todo: cache index meta
        db.selectIndexDefs(content.documentUri).flatMap { indexDefsIterator ⇒
          val indexDefs = indexDefsIterator.toSeq
          FutureUtils.serial(indexDefs) { indexDef ⇒
            log.debug(s"Removing index $indexDef")
            deleteIndexDefAndData(indexDef)
          } map (_ ⇒ {})
        }
      }
      else {
        val itemIds = incompleteTransactions.collect {
          case it if it.transaction.itemId.nonEmpty ⇒ it.transaction.itemId
        }.toSet

        // todo: cache index meta
        db.selectIndexDefs(content.documentUri).flatMap { indexDefsIterator ⇒
          val indexDefs = indexDefsIterator.toSeq
          FutureUtils.serial(itemIds.toSeq) { itemId ⇒
            // todo: cache content
            db.selectContent(content.documentUri, itemId) flatMap { contentOption ⇒
              if (log.isDebugEnabled) {
                log.debug(s"Indexing content $contentOption for $indexDefs")
              }
              FutureUtils.serial(indexDefs) { indexDef ⇒
                contentOption match {
                  case Some(item) ⇒
                    indexItem(indexDef, item)

                  case None ⇒
                    // todo: delete only if filter is true!
                    db.deleteIndexItem(indexDef.tableName, indexDef.documentUri, indexDef.indexId, itemId)
                }
              }
            }
          } map (_ ⇒ {})
        }
      }
    } else {
      Future.successful()
    }
  }

  def completeTransactions(task: BackgroundTask, content: ContentStatic): Future[ShardTaskComplete] = {
    if (content.transactionList.isEmpty) {
      Future.successful(ShardTaskComplete(task, BackgroundTaskResult(task.documentUri, Seq.empty)))
    }
    else {
      selectIncompleteTransactions(content) flatMap { incompleteTransactions ⇒
        val updateIndexFuture: Future[Unit] = updateIndexes(content, incompleteTransactions)
        updateIndexFuture.flatMap { _ ⇒
          FutureUtils.serial(incompleteTransactions) { it ⇒
            val event = it.unwrappedBody
            hyperbus <| event flatMap { publishResult ⇒
              if (log.isDebugEnabled) {
                log.debug(s"Event $event is published with result $publishResult")
              }
              db.completeTransaction(it.transaction) map { _ ⇒
                if (log.isDebugEnabled) {
                  log.debug(s"${it.transaction} is complete")
                }
                it.transaction
              }
            }
          }
        } map { updatedTransactions ⇒
          ShardTaskComplete(task, BackgroundTaskResult(task.documentUri, updatedTransactions.map(_.uuid)))
        } recover {
          case NonFatal(e) ⇒
            ShardTaskComplete(task, BackgroundTaskFailedException(task.documentUri, e.toString))
        } andThen {
          case Success(ShardTaskComplete(_, BackgroundTaskResult(documentUri, updatedTransactions))) ⇒
            log.debug(s"Removing completed transactions $updatedTransactions from $documentUri")
            db.removeCompleteTransactionsFromList(documentUri, updatedTransactions.toList) recover {
              case NonFatal(e) ⇒
                log.error(e, s"Can't remove complete transactions $updatedTransactions from $documentUri")
            }
        }
      }
    }
  }

  def selectIncompleteTransactions(content: ContentStatic): Future[Seq[UnwrappedTransaction]] = {
    val transactionsFStream = content.transactionList.toStream.map { transactionUuid ⇒
      val quantum = TransactionLogic.getDtQuantum(UUIDs.unixTimestamp(transactionUuid))
      db.selectTransaction(quantum, content.partition, content.documentUri, transactionUuid)
    }
    FutureUtils.collectWhile(transactionsFStream) {
      case Some(transaction) ⇒ UnwrappedTransaction(transaction)
    } map (_.reverse)
  }

  def executeIndexDefTask(owner: ActorRef, task: IndexDefTask): Unit = {
    Try {
      validateCollectionUri(task.key)
      task
    } map { task ⇒
      task.request match {
        case post: HyperStorageIndexPost ⇒ startCreatingNewIndex(task, owner, post)
        case delete: HyperStorageIndexDelete ⇒ startRemovingIndex(task, owner, delete)
      }
    } recover withHyperbusException(s"Can't execute index task: $task", owner, task)
  }

  def startCreatingNewIndex(task: BackgroundTaskTrait, owner: ActorRef, post: HyperStorageIndexPost): Unit = {
    implicit val mcx = post
    val indexId = post.body.indexId.getOrElse(
      IdGenerator.create()
    )

    val tableName = IndexLogic.tableName(post.body.sortBy)
    post.body.filterBy.foreach(IndexLogic.validateFilterExpression(_).get)

    db.selectIndexDefs(post.path) flatMap { indexDefs ⇒
      indexDefs.foreach { existingIndex ⇒
        if (existingIndex.indexId == indexId) {
          throw Conflict(ErrorBody("already-exists", Some(s"Index '$indexId' already exists")))
        }
      }

      import eu.inn.binders.json._
      val indexDef = IndexDef(post.path, indexId, IndexDef.STATUS_INDEXING,
        IndexLogic.serializeSortByFields(post.body.sortBy), post.body.filterBy, tableName, defTransactionId = UUIDs.timeBased()
      )
      val pendingIndex = PendingIndex(TransactionLogic.partitionFromUri(post.path), post.path, indexId, None, indexDef.defTransactionId)

      // validate: id, sort, expression, etc
      db.insertPendingIndex(pendingIndex) flatMap { _ ⇒
        db.insertIndexDef(indexDef) flatMap { _ ⇒
          implicit val timeout = Timeout(60.seconds)
          indexManager ? IndexManager.IndexCreatedOrDeleted(IndexDefTransaction(
            post.path,
            indexId,
            pendingIndex.defTransactionId
          )) map { _ ⇒ // IndexManager.IndexCommandAccepted
            Created(HyperStorageIndexCreated(indexId, path = post.path, links = new LinksBuilder()
              .location(HyperStorageIndex.selfPattern, templated = true)
              .result()
            ))
          }
        }
      }
    } map { result ⇒
      owner ! ShardTaskComplete(task, result)
    } recover withHyperbusException(s"Can't create index $post", owner, task)
  }

  def startRemovingIndex(task: BackgroundTaskTrait, owner: ActorRef, delete: HyperStorageIndexDelete): Unit = {
    implicit val mcx = delete

    db.selectIndexDef(delete.path, delete.indexId) flatMap {
      case Some(indexDef) if indexDef.status != IndexDef.STATUS_DELETING ⇒
        val pendingIndex = PendingIndex(TransactionLogic.partitionFromUri(delete.path), delete.path, delete.indexId, None, UUIDs.timeBased())
        db.insertPendingIndex(pendingIndex) flatMap { _ ⇒
          db.updateIndexDefStatus(pendingIndex.documentUri, pendingIndex.indexId, IndexDef.STATUS_DELETING, pendingIndex.defTransactionId) flatMap { _ ⇒
            implicit val timeout = Timeout(60.seconds)
            indexManager ? IndexManager.IndexCreatedOrDeleted(IndexDefTransaction(
              delete.path,
              delete.indexId,
              pendingIndex.defTransactionId
            )) map { _ ⇒ // IndexManager.IndexCommandAccepted
              NoContent(EmptyBody)
            }
          }
        }

      case _ ⇒ Future.successful(NotFound(ErrorBody("index-not-found", Some(s"Index ${delete.indexId} for ${delete.path} is not found"))))
    } map { result ⇒
      owner ! ShardTaskComplete(task, result)
    } recover withHyperbusException(s"Can't delete index $delete", owner, task)
  }

  def indexNextBucket(owner: ActorRef, task: IndexNextBucketTask): Unit = {
    Try {
      validateCollectionUri(task.key)
      task
    } map { task ⇒ {
      // todo: cache indexDef
      db.selectIndexDef(task.indexDefTransaction.documentUri, task.indexDefTransaction.indexId) flatMap {
        case Some(indexDef) if indexDef.defTransactionId == task.indexDefTransaction.defTransactionId ⇒ indexDef.status match {
          case IndexDef.STATUS_INDEXING ⇒
            val bucketSize = 1 // todo: move to config, or make adaptive, or per index

            db.selectContentCollection(task.indexDefTransaction.documentUri, bucketSize, task.lastItemId) flatMap { collectionItems ⇒
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
      }
    } map { r: IndexContentTaskResult ⇒
      owner ! ShardTaskComplete(task, r)
      // todo: remove ugly code duplication
    } recover withHyperbusException(s"Can't execute index task: $task", owner, task)
    } recover withHyperbusException(s"Can't execute index task: $task", owner, task)
  }

  def withHyperbusException(msg: String, owner: ActorRef, task: BackgroundTaskTrait): PartialFunction[Throwable, Unit] = {
    case NonFatal(e) ⇒
      log.error(e, msg)
      owner ! ShardTaskComplete(task, hyperbusException(e))
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

  def validateCollectionUri(uri: String) = {
    val ResourcePath(documentUri, itemId) = splitPath(uri)
    if (!ContentLogic.isCollectionUri(uri) || !itemId.isEmpty) {
      throw new IllegalArgumentException(s"Task key '$uri' isn't a collection URI.")
    }
    if (documentUri != uri) {
      throw new IllegalArgumentException(s"Task key '$uri' doesn't correspond to $documentUri")
    }
  }

  def indexItem(indexDef: IndexDef, item: Content): Future[String] = {
    if (log.isDebugEnabled) {
      log.debug(s"Indexing item $item with $indexDef")
    }

    import eu.inn.binders.json._

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

  def hyperbusException(e: Throwable): HyperbusException[ErrorBody] = {
    e match {
      case h: HyperbusException[ErrorBody] ⇒ h
      case other ⇒ InternalServerError(ErrorBody("failed", Some(e.toString)))
    }
  }
}

object BackgroundWorker {
  def props(hyperbus: Hyperbus, db: Db, tracker: MetricsTracker, indexManager: ActorRef) = Props(classOf[BackgroundWorker],
    hyperbus, db, tracker, indexManager
  )
}

case class UnwrappedTransaction(transaction: Transaction, unwrappedBody: DynamicRequest)

object UnwrappedTransaction {
  def apply(transaction: Transaction): UnwrappedTransaction = UnwrappedTransaction(
    transaction, DynamicRequest(transaction.body)
  )
}
