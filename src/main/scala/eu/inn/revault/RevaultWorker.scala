package eu.inn.revault

import java.util.Date

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.pattern.pipe
import eu.inn.binders.dynamic._
import eu.inn.hyperbus.transport.api.matchers.Specific
import eu.inn.hyperbus.transport.api.uri.Uri
import eu.inn.hyperbus.{IdGenerator, HyperBus}
import eu.inn.hyperbus.model._
import eu.inn.hyperbus.serialization.{StringDeserializer, StringSerializer}
import eu.inn.revault.db._
import eu.inn.revault.protocol.TransactionCreated
import eu.inn.revault.sharding.{ShardTask, ShardTaskComplete}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Try
import scala.util.control.NonFatal

@SerialVersionUID(1L) case class RevaultTask(key: String, ttl: Long, content: String) extends ShardTask {
  def isExpired = ttl < System.currentTimeMillis()
  def group = "revault"
}

@SerialVersionUID(1L) case class RevaultTaskResult(content: String)

case class RevaultWorkerTaskFailed(task: ShardTask, inner: Throwable)
case class RevaultWorkerTaskCompleted(task: ShardTask, transaction: Transaction, resourceCreated: Boolean)

// todo: rename this
class RevaultWorker(hyperBus: HyperBus, db: Db, completerTimeout: FiniteDuration) extends Actor with ActorLogging {
  import ContentLogic._
  import context._

  def receive = {
    case task: RevaultTask ⇒
      executeTask(sender(), task)
  }

  def executeTask(owner: ActorRef, task: RevaultTask): Unit = {
    Try{
      val request = DynamicRequest(task.content)
      val (documentUri, itemSegment) = splitPath(request.path)
      if (documentUri != task.key) {
        throw new IllegalArgumentException(s"RevaultWorker task key ${task.key} doesn't correspond to $documentUri")
      }
      val (updatedItemSegment, updatedRequest) = request.method match {
        case Method.POST ⇒
          // posting new item into collection, converting post to put
          val id = IdGenerator.create()
          if (itemSegment.isEmpty) {
            (id, request.copy(
              uri = Uri(request.uri.pattern, request.uri.args + "path" → Specific(request.path + "/" + id)),
              headers = new HeadersBuilder(request.headers) withMethod Method.PUT result(), // POST becomes PUT with auto Id
              body = appendId(filterNulls(request.body), id)
            ))
          }
          else {
            throw new IllegalArgumentException(s"RevaultWorker POST on collection item is not supported")
          }

        case Method.PUT ⇒
          if (itemSegment.isEmpty)
            (itemSegment, request.copy(body = filterNulls(request.body)))
          else
            (itemSegment, request.copy(body = appendId(filterNulls(request.body), itemSegment)))
        case _ ⇒
          (itemSegment, request)
      }
      (documentUri, updatedItemSegment, updatedRequest)
    } map {
      case (documentUri: String, itemSegment: String, request: DynamicRequest) ⇒
        become(taskWaitResult(owner, task, request)(request))

        // fetch and complete existing content
        executeResourceUpdateTask(owner, documentUri, itemSegment, task, request)
    } recover {
      case NonFatal(e) ⇒
        log.error(e, s"Can't deserialize and split path for: $task")
        owner ! ShardTaskComplete(task, hyperbusException(e, task))
    }
  }

  def executeResourceUpdateTask(owner: ActorRef, documentUri: String, itemSegment: String, task: RevaultTask, request: DynamicRequest) = {
    db.selectContent(documentUri, itemSegment) flatMap { existingContent ⇒
      val f: Future[Option[ContentBase]] = existingContent match {
        case Some(content) ⇒ Future.successful(Some(content))
        case None ⇒ db.selectContentStatic(documentUri)
      }

      f flatMap { existingContentStatic ⇒
        updateResource(documentUri, itemSegment, request, existingContent, existingContentStatic) map { newTransaction ⇒
          RevaultWorkerTaskCompleted(task, newTransaction, existingContent.isEmpty)
        }
      }
    } recover {
      case NonFatal(e) ⇒
        RevaultWorkerTaskFailed(task, e)
    } pipeTo context.self
  }

  private def createNewTransaction(documentUri: String, request: DynamicRequest, existingContent: Option[ContentBase]): Transaction = {
    val revision = existingContent match {
      case None ⇒ 1
      case Some(content) ⇒ content.revision + 1
    }
    TransactionLogic.newTransaction(documentUri, revision, request.copy(
      headers = Headers.plain(request.headers +
        (Header.REVISION → Seq(revision.toString)) +
        (Header.METHOD → Seq("feed:" + request.method)))
    ).serializeToString())
  }

  private def taskWaitResult(owner: ActorRef, originalTask: RevaultTask, request: DynamicRequest)(implicit mcf: MessagingContextFactory): Receive = {
    case RevaultWorkerTaskCompleted(task, transaction, created) if task == originalTask ⇒
      if (log.isDebugEnabled) {
        log.debug(s"RevaultWorker task $originalTask is completed")
      }
      owner ! RevaultCompleterTask(System.currentTimeMillis() + completerTimeout.toMillis, transaction.documentUri)
      val transactionId = transaction.documentUri + ":" + transaction.uuid + ":" + transaction.revision
      val result: Response[Body] = if (created) {
        Created(TransactionCreated(transactionId, path = request.path))
      }
      else {
        Ok(protocol.Transaction(transactionId))
      }
      owner ! ShardTaskComplete(task, RevaultTaskResult(result.serializeToString()))
      unbecome()

    case RevaultWorkerTaskFailed(task, e) if task == originalTask ⇒
      owner ! ShardTaskComplete(task, hyperbusException(e, task))
      unbecome()
  }

  private def hyperbusException(e: Throwable, task: ShardTask): RevaultTaskResult = {
    val (response:HyperBusException[ErrorBody], logException) = e match {
      case h: NotFound[ErrorBody] ⇒ (h, false)
      case h: HyperBusException[ErrorBody] ⇒ (h, true)
      case other ⇒ (InternalServerError(ErrorBody("update_failed",Some(e.toString))), true)
    }

    if (logException) {
      log.error(e, s"Task $task is failed")
    }

    RevaultTaskResult(response.serializeToString())
  }

  private def updateContent(documentUri: String,
                            itemSegment: String,
                            newTransaction: Transaction,
                            request: DynamicRequest,
                            existingContent: Option[Content],
                            existingContentStatic: Option[ContentBase]): Content =
  request.method match {
    case Method.PUT ⇒ putContent(documentUri, itemSegment, newTransaction, request, existingContent, existingContentStatic)
    case Method.PATCH ⇒ patchContent(documentUri, itemSegment, newTransaction, request, existingContent)
    case Method.DELETE ⇒ deleteContent(documentUri, itemSegment, newTransaction, request, existingContent)
  }

  private def putContent(documentUri: String,
                         itemSegment: String,
                         newTransaction: Transaction,
                         request: DynamicRequest,
                         existingContent: Option[Content],
                         existingContentStatic: Option[ContentBase]): Content = existingContentStatic match {
    case None ⇒
      Content(documentUri, itemSegment, newTransaction.revision,
        transactionList = List(newTransaction.uuid),
        body = Some(request.body.serializeToString()),
        isDeleted = false,
        createdAt = existingContent.map(_.createdAt).getOrElse(new Date),
        modifiedAt = existingContent.flatMap(_.modifiedAt)
      )

    case Some(static) ⇒
      Content(documentUri, itemSegment, newTransaction.revision,
        transactionList = newTransaction.uuid +: static.transactionList,
        body = Some(request.body.serializeToString()),
        isDeleted = false,
        createdAt = existingContent.map(_.createdAt).getOrElse(new Date),
        modifiedAt = existingContent.flatMap(_.modifiedAt)
      )
  }

  private def patchContent(documentUri: String,
                           itemSegment: String,
                           newTransaction: Transaction,
                           request: DynamicRequest,
                           existingContent: Option[Content]): Content = existingContent match {
    case None ⇒ {
      implicit val mcx = request
      throw NotFound(ErrorBody("not_found", Some(s"Resource '${request.path}' is not found")))
    }

    case Some(content) ⇒
      Content(documentUri, itemSegment, newTransaction.revision,
        transactionList = newTransaction.uuid +: content.transactionList,
        body = Some(mergeBody(StringDeserializer.dynamicBody(content.body), request.body).serializeToString()),
        isDeleted = false,
        createdAt = content.createdAt,
        modifiedAt = Some(new Date())
      )
  }

  private def deleteContent(documentUri: String,
                            itemSegment: String,
                            newTransaction: Transaction,
                            request: DynamicRequest,
                            existingContent: Option[Content]): Content = existingContent match {
    case None ⇒ {
      implicit val mcx = request
      throw NotFound(ErrorBody("not_found", Some(s"Resource '${request.path}' is not found")))
    }

    case Some(content) ⇒
      Content(documentUri, itemSegment, newTransaction.revision,
        transactionList = newTransaction.uuid +: content.transactionList,
        body = None,
        isDeleted = true,
        createdAt = content.createdAt,
        modifiedAt = Some(new Date())
      )
  }

  private def updateResource(documentUri: String,
                             itemSegment: String,
                             request: DynamicRequest,
                             existingContent: Option[Content],
                             existingContentStatic: Option[ContentBase]): Future[Transaction] = {
    val newTransaction = createNewTransaction(documentUri, request, existingContentStatic)
    val newContent = updateContent(documentUri, itemSegment, newTransaction, request, existingContent, existingContentStatic)
    db.insertTransaction(newTransaction) flatMap { _ ⇒
      db.insertContent(newContent) map { _ ⇒
        newTransaction
      }
    }
  }

  private def mergeBody(existing: DynamicBody, patch: DynamicBody): DynamicBody = {
    DynamicBody(filterNulls(existing.content.merge(patch.content)))
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

  val filterNullsVisitor = new ValueVisitor[Value]{
    override def visitNumber(d: Number): Value = d
    override def visitNull(): Value = Null
    override def visitBool(d: Bool): Value = d
    override def visitObj(d: Obj): Value = Obj(d.v.flatMap {
      case (k,Null) ⇒ None
      case (k,other) ⇒ Some(k → filterNulls(other))
    })
    override def visitText(d: Text): Value = d
    override def visitLst(d: Lst): Value = d
  }

  def filterNulls(content: Value): Value = {
    content.accept[Value](filterNullsVisitor)
  }

  def filterNulls(body: DynamicBody): DynamicBody = {
    body.copy(content = body.content.accept[Value](filterNullsVisitor))
  }

  def appendId(body: DynamicBody, id: Value): DynamicBody = {
    body.copy(content = Obj(body.content.asMap + ("id" → id)))
  }
}
