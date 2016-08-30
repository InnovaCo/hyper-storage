package eu.inn.hyperstorage.workers.secondary

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.pattern.ask
import akka.util.Timeout
import com.datastax.driver.core.utils.UUIDs
import com.fasterxml.jackson.core.JsonParser
import eu.inn.hyperbus.model._
import eu.inn.hyperbus.serialization.{MessageDeserializer, RequestDeserializer, RequestHeader, StringSerializer}
import eu.inn.hyperbus.{Hyperbus, IdGenerator}
import eu.inn.hyperstorage.api.{HyperStorageIndexPost, _}
import eu.inn.hyperstorage.db._
import eu.inn.hyperstorage.indexing.{IndexDefTransaction, IndexLogic, IndexManager}
import eu.inn.hyperstorage.sharding.ShardTaskComplete
import eu.inn.hyperstorage.{ResourcePath, _}
import eu.inn.metrics.MetricsTracker

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal


@SerialVersionUID(1L) case class IndexDefTask(ttl: Long, documentUri: String, content: String) extends SecondaryTaskTrait {
  def key = documentUri
}

@SerialVersionUID(1L) case class IndexDefTaskTaskResult(content: String)

trait IndexDefTaskWorker {
  def hyperbus: Hyperbus
  def db: Db
  def tracker: MetricsTracker
  def log: LoggingAdapter
  def indexManager: ActorRef
  implicit def executionContext: ExecutionContext

  def executeIndexDefTask(task: IndexDefTask): Future[ShardTaskComplete] = {
    {
      try {
        validateCollectionUri(task.key)
        val is = new java.io.ByteArrayInputStream(task.content.getBytes("UTF-8"))
        val deserializer: RequestDeserializer[Request[Body]] = (requestHeader: RequestHeader, jsonParser: JsonParser) ⇒ {
          requestHeader.method match {
            case Method.POST ⇒ HyperStorageIndexPost.apply(requestHeader, jsonParser)
            case Method.DELETE ⇒ HyperStorageIndexDelete.apply(requestHeader, jsonParser)
          }
        }

        MessageDeserializer.deserializeRequestWith[Request[Body]](is)(deserializer) match {
          case post: HyperStorageIndexPost ⇒ startCreatingNewIndex(task, post)
          case delete: HyperStorageIndexDelete ⇒ startRemovingIndex(task, delete)
        }
      } catch {
        case NonFatal(e) ⇒
          Future.failed(e)
      }
    } recover withHyperbusException(task)
  }

  def validateCollectionUri(uri: String) = {
    val ResourcePath(documentUri, itemId) = ContentLogic.splitPath(uri)
    if (!ContentLogic.isCollectionUri(uri) || !itemId.isEmpty) {
      throw new IllegalArgumentException(s"Task key '$uri' isn't a collection URI.")
    }
    if (documentUri != uri) {
      throw new IllegalArgumentException(s"Task key '$uri' doesn't correspond to $documentUri")
    }
  }

  private def startCreatingNewIndex(task: SecondaryTaskTrait, post: HyperStorageIndexPost): Future[ShardTaskComplete] = {
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
      ShardTaskComplete(task, result)
    }
  }

  private def startRemovingIndex(task: SecondaryTaskTrait, delete: HyperStorageIndexDelete): Future[ShardTaskComplete] = {
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
      ShardTaskComplete(task, result)
    }
  }

  private def withHyperbusException(task: SecondaryTaskTrait): PartialFunction[Throwable, ShardTaskComplete] = {
    case NonFatal(e) ⇒
      log.error(e, s"Can't execute $task")
      val he = e match {
        case h: HyperbusException[ErrorBody] ⇒ h
        case other ⇒ InternalServerError(ErrorBody("failed", Some(e.toString)))
      }
      ShardTaskComplete(task, IndexDefTaskTaskResult(StringSerializer.serializeToString(he)))
  }
}
