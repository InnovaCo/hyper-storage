package eu.inn.hyperstorage

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Status}
import akka.pattern.pipe
import com.datastax.driver.core.utils.UUIDs
import eu.inn.hyperbus.Hyperbus
import eu.inn.hyperbus.model._
import eu.inn.hyperstorage.api.{HyperStorageIndexDelete, HyperStorageIndexPost}
import eu.inn.metrics.MetricsTracker
import eu.inn.hyperstorage.db.{ContentStatic, Db, PendingIndex, Transaction}
import eu.inn.hyperstorage.metrics.Metrics
import eu.inn.hyperstorage.sharding.{ShardTask, ShardTaskComplete}
import eu.inn.hyperstorage.utils.FutureUtils
import org.slf4j.LoggerFactory

import scala.collection.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}
import scala.util.control.NonFatal

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

@SerialVersionUID(1L) case class IndexMetaTask(ttl: Long, request: Request[Body]) extends BackgroundTaskTrait {
  def key: String = request match {
    case post: HyperStorageIndexPost ⇒ post.path
    case delete: HyperStorageIndexDelete ⇒ delete.path
  }
}

@SerialVersionUID(1L) case class IndexMetaTaskResult(response: Response[Body])

@SerialVersionUID(1L) case class IndexTask(ttl: Long, indexMeta: db.IndexMeta, lastItemSegment: Option[String], processId: Long) extends BackgroundTaskTrait {
  def key = indexMeta.documentUri
}

@SerialVersionUID(1L) case class IndexTaskResult(lastItemSegment: Option[String], processId: Long)

@SerialVersionUID(1L) case class NoSuchResourceException(documentUri: String) extends RuntimeException(s"No such resource: $documentUri")
@SerialVersionUID(1L) case class IncorrectDataException(documentUri: String, reason: String) extends RuntimeException(s"Data for $documentUri is incorrect: $reason")
@SerialVersionUID(1L) case class BackgroundTaskFailedException(documentUri: String, reason: String) extends RuntimeException(s"Background task for $documentUri is failed: $reason")

class BackgroundWorker(hyperbus: Hyperbus, db: Db, tracker: MetricsTracker) extends Actor with ActorLogging {
  import ContentLogic._
  import context._

  override def receive: Receive = {
    case task: BackgroundTask ⇒
      executeBackgroundTask(sender(), task)

    case task: IndexMetaTask ⇒
      executeIndexMetaTask(sender(), task)

    case task: IndexTask ⇒
      executeIndexTask(sender(), task)
  }

  def executeBackgroundTask(owner: ActorRef, task: BackgroundTask): Unit = {
    val ResourcePath(documentUri, itemSegment) = ContentLogic.splitPath(task.documentUri)
    if (!itemSegment.isEmpty) {
      owner ! Status.Success { // todo: is this have to be a success
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

  def completeTransactions(task: BackgroundTask, content: ContentStatic): Future[ShardTaskComplete] = {
    if (content.transactionList.isEmpty) {
      Future.successful(ShardTaskComplete(task, BackgroundTaskResult(task.documentUri, Seq.empty)))
    }
    else {
      selectIncompleteTransactions(content) flatMap { incompleteTransactions ⇒
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

  def executeIndexMetaTask(owner: ActorRef, task: IndexMetaTask): Unit = {
    Try {
      validateCollectionUri(task.key)
      task
    } map { task ⇒
      import context.dispatcher
      task.request match {
        case post: HyperStorageIndexPost ⇒ BackgroundWorkerImpl.createNewIndex(context.self, owner, post, db)
        case delete: HyperStorageIndexDelete ⇒ BackgroundWorkerImpl.deleteIndex(context.self, owner, delete, db)
      }
    } recover {
      case NonFatal(e) ⇒
        log.error(e, s"Can't execute index task: $task")
        owner ! ShardTaskComplete(task, BackgroundWorkerImpl.hyperbusException(e))
    }
  }


  def validateCollectionUri(uri: String) = {
    val ResourcePath(documentUri, itemSegment) = splitPath(uri)
    if (!uri.endsWith("~") || !itemSegment.isEmpty) {
      throw new IllegalArgumentException(s"Task key '$uri' isn't a collection URI.")
    }
    if (documentUri != uri) {
      throw new IllegalArgumentException(s"Task key '$uri' doesn't correspond to $documentUri")
    }
  }

  def executeIndexTask(owner: ActorRef, task: IndexTask): Unit = {
    Try {
      validateCollectionUri(task.key)
      task
    } map { task ⇒

    } recover {
      case NonFatal(e) ⇒
        log.error(e, s"Can't execute index task: $task")
        owner ! ShardTaskComplete(task, BackgroundWorkerImpl.hyperbusException(e))
    }
  }
}

object BackgroundWorker {
  def props(hyperbus: Hyperbus, db: Db, tracker: MetricsTracker) = Props(classOf[BackgroundWorker],
    hyperbus, db, tracker
  )
}

object BackgroundWorkerImpl {
  val log = LoggerFactory.getLogger(getClass)

  def createNewIndex(notifyActor: ActorRef, owner: ActorRef, post: HyperStorageIndexPost, db: Db)
                    (implicit ec: ExecutionContext): Unit = {
/*
    db.selectIndexMetas(post.path) map { indexMetas ⇒
      indexMetas.map { existingIndex ⇒
        if (existingIndex.indexId == post.body.indexId) {
          throw
        }
      }


    } recover {
      case NonFatal(e) ⇒
        log.error(s"Can't delete pending index $pendingIndex", e)
        hyperbusException(e)
  */

    /*
    1. validate: id, sort, expression, etc
    2. fetch indexes and:
      2.1. check if id is unique
      2.2. check if exists same index with other id
    3. insert pending index
    4. insert meta index
    5. notify index manager
     */
    ???
  }

  def deleteIndex(notifyActor: ActorRef, owner: ActorRef, delete: HyperStorageIndexDelete, db: Db)
                 (implicit ec: ExecutionContext): Unit = {
    /*
      1. fetch indexes (404)
      2. insert pending index
      3. update meta index
      4. notify index worker
    */

    ???
  }

  def hyperbusException(e: Throwable): IndexMetaTaskResult = {
    val response:HyperbusException[ErrorBody] = e match {
      case h: HyperbusException[ErrorBody] ⇒ h
      case other ⇒ InternalServerError(ErrorBody("failed",Some(e.toString)))
    }
    IndexMetaTaskResult(response)
  }
}
