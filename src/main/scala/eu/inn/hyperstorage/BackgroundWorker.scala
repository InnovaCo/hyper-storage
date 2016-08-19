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
import eu.inn.hyperstorage.indexing.{IndexCreatedOrDeleted, IndexDefTransaction, IndexLogic}
import eu.inn.hyperstorage.metrics.Metrics
import eu.inn.hyperstorage.sharding.{ShardTask, ShardTaskComplete}
import eu.inn.hyperstorage.utils.FutureUtils
import eu.inn.metrics.MetricsTracker

import scala.collection.Seq
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.{Success, Try}

// todo: do we really need a ShardTaskComplete ?
// todo: use strictly Hyperbus OR akka serialization for the internal akka-cluster
// todo: collection removal + index removal

trait BackgroundTaskTrait extends ShardTask {
  def ttl: Long

  def isExpired = ttl < System.currentTimeMillis()

  def group = "hyper-storage-background-worker"
}

@SerialVersionUID(1L) case class BackgroundTask(ttl: Long, documentUri: String) extends BackgroundTaskTrait {
  def key = documentUri
}

@SerialVersionUID(1L) case class BackgroundTaskResult(documentUri: String, transactions: Seq[UUID])

// todo: request -> string as for the Foreground
@SerialVersionUID(1L) case class IndexDefTask(ttl: Long, request: Request[Body]) extends BackgroundTaskTrait {
  def key: String = request match {
    case post: HyperStorageIndexPost ⇒ post.path
    case delete: HyperStorageIndexDelete ⇒ delete.path
  }
}

@SerialVersionUID(1L) case class IndexContentTask(ttl: Long, indexDefTransaction: IndexDefTransaction, lastItemSegment: Option[String], processId: Long) extends BackgroundTaskTrait {
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

    case task: IndexContentTask ⇒
      executeIndexContentTask(sender(), task)
  }

  def executeBackgroundTask(owner: ActorRef, task: BackgroundTask): Unit = {
    val ResourcePath(documentUri, itemSegment) = ContentLogic.splitPath(task.documentUri)
    if (!itemSegment.isEmpty) {
      // todo: is this have to be a success
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

  // todo: index new updates
  def completeTransactions(task: BackgroundTask, content: ContentStatic): Future[ShardTaskComplete] = {
    if (content.transactionList.isEmpty) {
      Future.successful(ShardTaskComplete(task, BackgroundTaskResult(task.documentUri, Seq.empty)))
    }
    else {
      selectIncompleteTransactions(content) flatMap { incompleteTransactions ⇒

        val itemSegments = incompleteTransactions.collect {
          case it if it.itemSegment.nonEmpty ⇒ it.itemSegment
        }.toSet

        // todo: cache index meta
        val updateIndexFuture: Future[Seq[Int]] = db.selectIndexDefs(task.documentUri).flatMap { indexDefsIterator ⇒
          val indexDefs = indexDefsIterator.toSeq
          FutureUtils.serial(itemSegments.toSeq) { itemSegment ⇒
            // todo: cache content
            db.selectContent(task.documentUri, itemSegment) flatMap { contentOption ⇒
              if (log.isDebugEnabled) {
                log.debug(s"Indexing content $contentOption for $indexDefs")
              }
              FutureUtils.serial(indexDefs.toSeq) { indexDef ⇒
                contentOption match {
                  case Some(item) ⇒
                    indexItem(indexDef, item)

                  case None ⇒
                    // todo: delete only if filter is true!
                    db.deleteIndexItem(indexDef.tableName, indexDef.documentUri, indexDef.indexId, itemSegment)
                }
              } map (_.size)
            }
          }


        }

        updateIndexFuture.flatMap { indexSeq ⇒
          FutureUtils.serial(incompleteTransactions) { transaction ⇒
            val event = DynamicRequest(transaction.body)
            hyperbus <| event flatMap { publishResult ⇒
              if (log.isDebugEnabled) {
                log.debug(s"Event $event is published with result $publishResult")
              }
              db.completeTransaction(transaction) map { _ ⇒
                if (log.isDebugEnabled) {
                  log.debug(s"$transaction is complete")
                }
                transaction
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

  def selectIncompleteTransactions(content: ContentStatic): Future[Seq[Transaction]] = {
    val transactionsFStream = content.transactionList.toStream.map { transactionUuid ⇒
      val quantum = TransactionLogic.getDtQuantum(UUIDs.unixTimestamp(transactionUuid))
      db.selectTransaction(quantum, content.partition, content.documentUri, transactionUuid)
    }
    FutureUtils.collectWhile(transactionsFStream) {
      case Some(transaction) ⇒ transaction
    } map (_.reverse)
  }

  def executeIndexDefTask(owner: ActorRef, task: IndexDefTask): Unit = {
    Try {
      validateCollectionUri(task.key)
      task
    } map { task ⇒
      task.request match {
        case post: HyperStorageIndexPost ⇒ createNewIndex(task, owner, post)
        case delete: HyperStorageIndexDelete ⇒ deleteIndex(task, owner, delete)
      }
    } recover {
      case NonFatal(e) ⇒
        log.error(e, s"Can't execute index task: $task")
        owner ! ShardTaskComplete(task, hyperbusException(e))
    }
  }

  def createNewIndex(task: BackgroundTaskTrait, owner: ActorRef, post: HyperStorageIndexPost): Unit = {
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
        if (post.body.sortBy.nonEmpty) Some(post.body.sortBy.toJson) else None, post.body.filterBy, tableName, defTransactionId = UUIDs.timeBased()
      )
      val pendingIndex = PendingIndex(TransactionLogic.partitionFromUri(post.path), post.path, indexId, None, indexDef.defTransactionId)

      // validate: id, sort, expression, etc
      db.insertPendingIndex(pendingIndex) flatMap { _ ⇒
        db.insertIndexDef(indexDef) flatMap { _ ⇒
          implicit val timeout = Timeout(60.seconds)
          indexManager ? IndexCreatedOrDeleted(IndexDefTransaction(
            post.path,
            indexId,
            pendingIndex.defTransactionId
          )) map { _ ⇒
            Created(HyperStorageIndexCreated(indexId, path = post.path, links = new LinksBuilder()
              .location(HyperStorageIndex.selfPattern, templated = true)
              .result()
            ))
          }
        }
      }
    } recover {
      case NonFatal(e) ⇒
        log.error(s"Can't create index $post", e)
        hyperbusException(e)
    } map { result ⇒
      owner ! ShardTaskComplete(task, result)
    }
  }

  def deleteIndex(task: BackgroundTaskTrait, owner: ActorRef, delete: HyperStorageIndexDelete): Unit = {
    /*
      1. fetch indexes (404)
      2. insert pending index
      3. update meta index
      4. notify index worker
    */

    implicit val mcx = delete

    db.selectIndexDef(delete.path, delete.indexId) flatMap {
      case Some(indexDef) if indexDef.status != IndexDef.STATUS_DELETING ⇒
        val pendingIndex = PendingIndex(TransactionLogic.partitionFromUri(delete.path), delete.path, delete.indexId, None, UUIDs.timeBased())
        db.insertPendingIndex(pendingIndex) flatMap { _ ⇒
          db.updateIndexDefStatus(pendingIndex.documentUri, pendingIndex.indexId, IndexDef.STATUS_DELETING, pendingIndex.defTransactionId) flatMap { _ ⇒
            implicit val timeout = Timeout(60.seconds)
            indexManager ? IndexCreatedOrDeleted(IndexDefTransaction(
              delete.path,
              delete.indexId,
              pendingIndex.defTransactionId
            )) map { _ ⇒
              NoContent(EmptyBody)
            }
          }
        }

      case _ ⇒ Future.successful(NotFound(ErrorBody("index-not-found", Some(s"Index ${delete.indexId} for ${delete.path} is not found"))))
    } recover {
      // todo: remove code duplication
      case NonFatal(e) ⇒
        log.error(s"Can't delete index $delete", e)
        hyperbusException(e)
    } map { result ⇒
      owner ! ShardTaskComplete(task, result)
    }
  }

  def executeIndexContentTask(owner: ActorRef, task: IndexContentTask): Unit = {
    Try {
      validateCollectionUri(task.key)
      task
    } map { task ⇒ {
      // todo: cache indexDef
      db.selectIndexDef(task.indexDefTransaction.documentUri, task.indexDefTransaction.indexId) flatMap {
        case Some(indexDef) if indexDef.defTransactionId == task.indexDefTransaction.defTransactionId ⇒ indexDef.status match {
          case IndexDef.STATUS_INDEXING ⇒
            val bucketSize = 1 // todo: move to config, or make adaptive, or per index

            task.lastItemSegment.map { s ⇒
              db.selectContentCollectionFrom(task.indexDefTransaction.documentUri, s, bucketSize)
            } getOrElse {
              db.selectContentCollection(task.indexDefTransaction.documentUri, bucketSize)
            } flatMap { collectionItems ⇒
              FutureUtils.serial(collectionItems.toSeq) { item ⇒
                indexItem(indexDef, item)
              } flatMap { insertedItemSegments ⇒

                if (insertedItemSegments.isEmpty) {
                  // indexing is finished
                  // todo: fix code format
                  db.updateIndexDefStatus(task.indexDefTransaction.documentUri, task.indexDefTransaction.indexId, IndexDef.STATUS_NORMAL, task.indexDefTransaction.defTransactionId) flatMap { _ ⇒
                    db.deletePendingIndex(TransactionLogic.partitionFromUri(task.indexDefTransaction.documentUri), task.indexDefTransaction.documentUri, task.indexDefTransaction.indexId, task.indexDefTransaction.defTransactionId) map { _ ⇒
                      IndexContentTaskResult(None, task.processId)
                    }
                  }
                } else {
                  val last = insertedItemSegments.last
                  db.updatePendingIndexLastItemSegment(TransactionLogic.partitionFromUri(task.indexDefTransaction.documentUri), task.indexDefTransaction.documentUri, task.indexDefTransaction.indexId, task.indexDefTransaction.defTransactionId, last) map { _ ⇒
                    IndexContentTaskResult(Some(last), task.processId)
                  }
                }
              }
            }


          case IndexDef.STATUS_NORMAL ⇒
            Future.successful(IndexContentTaskResult(None, task.processId))

          case IndexDef.STATUS_DELETING ⇒
            db.deleteIndex(indexDef.tableName, indexDef.documentUri, indexDef.indexId) flatMap { _ ⇒
              db.deleteIndexDef(indexDef.documentUri, indexDef.indexId) flatMap { _ ⇒
                db.deletePendingIndex(
                  TransactionLogic.partitionFromUri(indexDef.documentUri), indexDef.documentUri, indexDef.indexId, indexDef.defTransactionId
                ) map { _ ⇒
                  IndexContentTaskResult(None, task.processId)
                }
              }
            }
        }

        case _ ⇒
          Future.failed(IndexContentTaskFailed(task.processId, s"Can't find index for ${task.indexDefTransaction}")) // todo: test this
      }
    } map { r: IndexContentTaskResult ⇒
      owner ! ShardTaskComplete(task, r)
      // todo: remove ugly code duplication
    } recover {
      case NonFatal(e) ⇒
        log.error(e, s"Can't execute index task: $task")
        owner ! ShardTaskComplete(task, e)
    }
    } recover {
      case NonFatal(e) ⇒
        log.error(e, s"Can't execute index task: $task")
        owner ! ShardTaskComplete(task, hyperbusException(e))
    }
  }

  def validateCollectionUri(uri: String) = {
    val ResourcePath(documentUri, itemSegment) = splitPath(uri)
    if (!ContentLogic.isCollectionUri(uri) || !itemSegment.isEmpty) {
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
      val sortBy = sortString.parseJson[Seq[HyperStorageIndexSortItem]]
      IndexLogic.extractSortFields(sortBy, contentValue)
    } getOrElse {
      Seq.empty
    }

    val write: Boolean = indexDef.filterBy.map { filterBy ⇒
      IndexLogic.evaluateFilterExpression(filterBy, contentValue) recover {
        case NonFatal(e) ⇒
          // todo: log this?
          log.error(e, s"Can't evaluate expression: `$filterBy`")
          false
      } get
    } getOrElse {
      true
    }

    if (log.isDebugEnabled) {
      log.debug(s"Evaluating: ${indexDef.filterBy}=$write on $item")
    }

    if (write) {
      val indexContent = IndexContent(
        item.documentUri, indexDef.indexId, item.itemSegment, item.revision, item.body, item.createdAt, item.modifiedAt
      )
      db.insertIndexItem(indexDef.tableName, sortBy, indexContent) map { _ ⇒
        item.itemSegment
      }
    }
    else {
      Future.successful(item.itemSegment)
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
