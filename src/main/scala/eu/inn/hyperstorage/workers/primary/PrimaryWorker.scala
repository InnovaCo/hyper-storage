package eu.inn.hyperstorage.workers.primary

import java.util.Date

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.pipe
import com.codahale.metrics.Timer
import eu.inn.binders.value._
import eu.inn.hyperbus.model._
import eu.inn.hyperbus.serialization.{StringDeserializer, StringSerializer}
import eu.inn.hyperbus.transport.api.matchers.Specific
import eu.inn.hyperbus.transport.api.uri.Uri
import eu.inn.hyperbus.{Hyperbus, IdGenerator}
import eu.inn.hyperstorage._
import eu.inn.hyperstorage.api._
import eu.inn.hyperstorage.db._
import eu.inn.hyperstorage.metrics.Metrics
import eu.inn.hyperstorage.sharding.{ShardTask, ShardTaskComplete}
import eu.inn.hyperstorage.workers.secondary.BackgroundContentTask
import eu.inn.metrics.MetricsTracker

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Try
import scala.util.control.NonFatal

@SerialVersionUID(1L) case class PrimaryTask(key: String, ttl: Long, content: String) extends ShardTask {
  def isExpired = ttl < System.currentTimeMillis()

  def group = "hyper-storage-primary-worker"
}

@SerialVersionUID(1L) case class PrimaryWorkerTaskResult(content: String)

case class PrimaryWorkerTaskFailed(task: ShardTask, inner: Throwable)

case class PrimaryWorkerTaskCompleted(task: ShardTask, transaction: Transaction, resourceCreated: Boolean)

class PrimaryWorker(hyperbus: Hyperbus, db: Db, tracker: MetricsTracker, backgroundTaskTimeout: FiniteDuration) extends Actor with ActorLogging {

  import ContentLogic._
  import context._

  val filterNullsVisitor = new ValueVisitor[Value] {
    override def visitNumber(d: Number): Value = d

    override def visitNull(): Value = Null

    override def visitBool(d: Bool): Value = d

    override def visitObj(d: Obj): Value = Obj(d.v.flatMap {
      case (k, Null) ⇒ None
      case (k, other) ⇒ Some(k → filterNulls(other))
    })

    override def visitText(d: Text): Value = d

    override def visitLst(d: Lst): Value = d
  }

  def receive = {
    case task: PrimaryTask ⇒
      executeTask(sender(), task)
  }

  def executeTask(owner: ActorRef, task: PrimaryTask): Unit = {
    val trackProcessTime = tracker.timer(Metrics.PRIMARY_PROCESS_TIME).time()

    Try {
      val request = DynamicRequest(task.content)
      val ResourcePath(documentUri, itemId) = splitPath(request.path)
      if (documentUri != task.key) {
        throw new IllegalArgumentException(s"Task key ${task.key} doesn't correspond to $documentUri")
      }
      val (updatedItemId, updatedRequest) = request.method match {
        case Method.POST ⇒
          // posting new item into collection, converting post to put
          val id = IdGenerator.create()
          if (ContentLogic.isCollectionUri(documentUri) && itemId.isEmpty) {
            (id, request.copy(
              uri = Uri(request.uri.pattern, request.uri.args + "path" → Specific(request.path + "/" + id)),
              headers = new HeadersBuilder(request.headers) withMethod Method.PUT result(), // POST becomes PUT with auto Id
              body = appendId(filterNulls(request.body), id)
            ))
          }
          else {
            // todo: replace with BadRequest?
            throw new IllegalArgumentException(s"POST is allowed only for a collection~")
          }

        case Method.PUT ⇒
          if (itemId.isEmpty)
            (itemId, request.copy(body = filterNulls(request.body)))
          else
            (itemId, request.copy(body = appendId(filterNulls(request.body), itemId)))
        case _ ⇒
          (itemId, request)
      }
      (documentUri, updatedItemId, updatedRequest)
    } map {
      case (documentUri: String, itemId: String, request: DynamicRequest) ⇒
        become(taskWaitResult(owner, task, request, trackProcessTime)(request))

        // fetch and complete existing content
        executeResourceUpdateTask(owner, documentUri, itemId, task, request)
    } recover {
      case NonFatal(e) ⇒
        log.error(e, s"Can't deserialize and split path for: $task")
        owner ! ShardTaskComplete(task, hyperbusException(e, task))
    }
  }

  private def executeResourceUpdateTask(owner: ActorRef, documentUri: String, itemId: String, task: PrimaryTask, request: DynamicRequest) = {
    db.selectContent(documentUri, itemId) flatMap { existingContent ⇒
      val f: Future[Option[ContentBase]] = existingContent match {
        case Some(content) ⇒ Future.successful(Some(content))
        case None ⇒ db.selectContentStatic(documentUri)
      }

      f flatMap { existingContentStatic ⇒
        updateResource(documentUri, itemId, request, existingContent, existingContentStatic) map { newTransaction ⇒
          PrimaryWorkerTaskCompleted(task, newTransaction, existingContent.isEmpty && request.method != Method.DELETE)
        }
      }
    } recover {
      case NonFatal(e) ⇒
        PrimaryWorkerTaskFailed(task, e)
    } pipeTo context.self
  }

  private def updateResource(documentUri: String,
                             itemId: String,
                             request: DynamicRequest,
                             existingContent: Option[Content],
                             existingContentStatic: Option[ContentBase]): Future[Transaction] = {
    val newTransaction = createNewTransaction(documentUri, itemId, request, existingContentStatic)
    val newContent = updateContent(documentUri, itemId, newTransaction, request, existingContent, existingContentStatic)
    db.insertTransaction(newTransaction) flatMap { _ ⇒ {
      if (!itemId.isEmpty && newContent.isDeleted) {
        // deleting item
        db.deleteContentItem(newContent, itemId)
      }
      else {
        db.insertContent(newContent)
      }
    } map { _ ⇒
      newTransaction
    }
    }
  }

  private def createNewTransaction(documentUri: String, itemId: String, request: DynamicRequest, existingContent: Option[ContentBase]): Transaction = {
    val revision = existingContent match {
      case None ⇒ 1
      case Some(content) ⇒ content.revision + 1
    }
    TransactionLogic.newTransaction(documentUri, itemId, revision, request.copy(
      headers = Headers.plain(request.headers +
        (Header.REVISION → Seq(revision.toString)) +
        (Header.METHOD → Seq("feed:" + request.method)))
    ).serializeToString())
  }

  private def updateContent(documentUri: String,
                            itemId: String,
                            newTransaction: Transaction,
                            request: DynamicRequest,
                            existingContent: Option[Content],
                            existingContentStatic: Option[ContentBase]): Content =
    request.method match {
      case Method.PUT ⇒ putContent(documentUri, itemId, newTransaction, request, existingContent, existingContentStatic)
      case Method.PATCH ⇒ patchContent(documentUri, itemId, newTransaction, request, existingContent)
      case Method.DELETE ⇒ deleteContent(documentUri, itemId, newTransaction, request, existingContent, existingContentStatic)
    }

  private def putContent(documentUri: String,
                         itemId: String,
                         newTransaction: Transaction,
                         request: DynamicRequest,
                         existingContent: Option[Content],
                         existingContentStatic: Option[ContentBase]): Content = {
    if (ContentLogic.isCollectionUri(documentUri) && itemId.isEmpty) {
      throw Conflict(ErrorBody("collection-put-not-implemented", Some(s"Currently you can't put the whole collection")))
    }

    existingContentStatic match {
      case None ⇒
        Content(documentUri, itemId, newTransaction.revision,
          transactionList = List(newTransaction.uuid),
          body = Some(request.body.serializeToString()),
          isDeleted = false,
          createdAt = existingContent.map(_.createdAt).getOrElse(new Date),
          modifiedAt = existingContent.flatMap(_.modifiedAt)
        )

      case Some(static) ⇒
        Content(documentUri, itemId, newTransaction.revision,
          transactionList = newTransaction.uuid +: static.transactionList,
          body = Some(request.body.serializeToString()),
          isDeleted = false,
          createdAt = existingContent.map(_.createdAt).getOrElse(new Date),
          modifiedAt = existingContent.flatMap(_.modifiedAt)
        )
    }
  }

  private def patchContent(documentUri: String,
                           itemId: String,
                           newTransaction: Transaction,
                           request: DynamicRequest,
                           existingContent: Option[Content]): Content = {
    if (ContentLogic.isCollectionUri(documentUri) && itemId.isEmpty) {
      throw new IllegalArgumentException(s"PATCH is not allowed for a collection~")
    }

    existingContent match {
      case Some(content) if !content.isDeleted ⇒
        Content(documentUri, itemId, newTransaction.revision,
          transactionList = newTransaction.uuid +: content.transactionList,
          body = Some(mergeBody(StringDeserializer.dynamicBody(content.body), request.body).serializeToString()),
          isDeleted = false,
          createdAt = content.createdAt,
          modifiedAt = Some(new Date())
        )

      case _ ⇒
        implicit val mcx = request
        throw NotFound(ErrorBody("not_found", Some(s"Resource '${request.path}' is not found")))
    }
  }

  private def mergeBody(existing: DynamicBody, patch: DynamicBody): DynamicBody = {
    DynamicBody(filterNulls(existing.content + patch.content))
  }

  def filterNulls(content: Value): Value = {
    content ~~ filterNullsVisitor
  }

  private def deleteContent(documentUri: String,
                            itemId: String,
                            newTransaction: Transaction,
                            request: DynamicRequest,
                            existingContent: Option[Content],
                            existingContentStatic: Option[ContentBase]): Content = existingContentStatic match {
    case None ⇒
      implicit val mcx = request
      throw NotFound(ErrorBody("not_found", Some(s"Resource '${request.path}' is not found")))

    case Some(content) ⇒
      Content(documentUri, itemId, newTransaction.revision,
        transactionList = newTransaction.uuid +: content.transactionList,
        body = None,
        isDeleted = true,
        createdAt = existingContent.map(_.createdAt).getOrElse(new Date()),
        modifiedAt = Some(new Date())
      )
  }

  private def taskWaitResult(owner: ActorRef, originalTask: PrimaryTask, request: DynamicRequest, trackProcessTime: Timer.Context)
                            (implicit mcf: MessagingContextFactory): Receive = {
    case PrimaryWorkerTaskCompleted(task, transaction, created) if task == originalTask ⇒
      if (log.isDebugEnabled) {
        log.debug(s"task $originalTask is completed")
      }
      owner ! BackgroundContentTask(System.currentTimeMillis() + backgroundTaskTimeout.toMillis, transaction.documentUri)
      val transactionId = transaction.documentUri + ":" + transaction.uuid + ":" + transaction.revision
      val result: Response[Body] = if (created) {
        Created(HyperStorageTransactionCreated(transactionId,
          path = request.path,
          links = new LinksBuilder()
            .self(api.HyperStorageTransaction.selfPattern, templated = true)
            .location(HyperStorageContentGet.uriPattern)
            .result()
        ))
      }
      else {
        Ok(api.HyperStorageTransaction(transactionId))
      }
      owner ! ShardTaskComplete(task, PrimaryWorkerTaskResult(result.serializeToString()))
      trackProcessTime.stop()
      unbecome()

    case PrimaryWorkerTaskFailed(task, e) if task == originalTask ⇒
      owner ! ShardTaskComplete(task, hyperbusException(e, task))
      trackProcessTime.stop()
      unbecome()
  }

  private def hyperbusException(e: Throwable, task: ShardTask): PrimaryWorkerTaskResult = {
    val (response: HyperbusException[ErrorBody], logException) = e match {
      case h: NotFound[ErrorBody] ⇒ (h, false)
      case h: HyperbusException[ErrorBody] ⇒ (h, true)
      case other ⇒ (InternalServerError(ErrorBody("update-failed", Some(e.toString))), true)
    }

    if (logException) {
      log.error(e, s"task $task is failed")
    }

    PrimaryWorkerTaskResult(response.serializeToString())
  }

  private def filterNulls(body: DynamicBody): DynamicBody = {
    body.copy(content = body.content ~~ filterNullsVisitor)
  }

  private def appendId(body: DynamicBody, id: Value): DynamicBody = {
    body.copy(content = Obj(body.content.asMap + ("id" → id)))
  }

  implicit class RequestWrapper(val request: DynamicRequest) {
    def path: String = request.uri.args("path").specific

    def isEvent = request.uri.pattern.specific.endsWith("/feed")

    def serializeToString(): String = StringSerializer.serializeToString(request)
  }

  implicit class ResponseWrapper(val response: Response[Body]) {
    def serializeToString(): String = StringSerializer.serializeToString(response)
  }

  implicit class BodyWrapper(val body: Body) {
    def serializeToString(): String = {
      StringSerializer.serializeToString(body)
    }
  }
}

object PrimaryWorker {
  def props(hyperbus: Hyperbus, db: Db, tracker: MetricsTracker, backgroundTaskTimeout: FiniteDuration) =
    Props(classOf[PrimaryWorker], hyperbus, db, tracker, backgroundTaskTimeout)
}
