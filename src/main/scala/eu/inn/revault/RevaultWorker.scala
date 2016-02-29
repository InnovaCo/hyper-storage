package eu.inn.revault

import java.io.OutputStream
import java.util.Date

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.pattern.pipe
import akka.util.Timeout
import eu.inn.binders.dynamic._
import eu.inn.hyperbus.HyperBus
import eu.inn.hyperbus.model.LinksMap.LinksMapType
import eu.inn.hyperbus.model._
import eu.inn.hyperbus.serialization.{StringDeserializer,StringSerializer}
import eu.inn.revault.db.{Content, Db, Transaction}
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
case class RevaultWorkerTaskAccepted(task: ShardTask, transaction: Transaction, resourceCreated: Boolean)

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
      (documentUri, itemSegment, request)
    } map {
      case (documentUri: String, itemSegment: String, request: DynamicRequest) ⇒
        become(taskWaitResult(owner, task)(request))

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
      updateResource(documentUri, itemSegment, request, existingContent) map { newTransaction ⇒
        RevaultWorkerTaskAccepted(task, newTransaction, existingContent.isEmpty)
      }
    } recover {
      case NonFatal(e) ⇒
        RevaultWorkerTaskFailed(task, e)
    } pipeTo context.self
  }

  private def createNewTransaction(request: DynamicRequest, existingContent: Option[Content]): Transaction = {
    val revision = existingContent match {
      case None ⇒ 1
      case Some(content) ⇒ content.revision + 1
    }
    TransactionLogic.newTransaction(request.path, revision, request.copy(
      headers = Headers.plain(request.headers +
        (Header.REVISION → Seq(revision.toString)) +
        (Header.METHOD → Seq("feed:" + request.method)))
    ).serializeToString())
  }

  private def taskWaitResult(owner: ActorRef, originalTask: RevaultTask)(implicit mcf: MessagingContextFactory): Receive = {
    case RevaultWorkerTaskAccepted(task, transaction, created) if task == originalTask ⇒
      if (log.isDebugEnabled) {
        log.debug(s"Task $originalTask is accepted")
      }
      owner ! RevaultCompleterTask(task.key, System.currentTimeMillis() + completerTimeout.toMillis, transaction.uri)
      val transactionId = transaction.uri + ":" + transaction.revision
      val result: Response[Body] = if (created) {
        Created(TransactionCreated(transactionId))
      }
      else {
        Ok(TransactionCreated(transactionId))
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
                            existingContent: Option[Content]): Content =
  request.method match {
    case Method.PUT ⇒ putContent(documentUri, itemSegment, newTransaction, request, existingContent)
    case Method.PATCH ⇒ patchContent(documentUri, itemSegment, newTransaction, request, existingContent)
    case Method.DELETE ⇒ deleteContent(documentUri, itemSegment, newTransaction, request, existingContent)
  }

  private def putContent(documentUri: String,
                         itemSegment: String,
                         newTransaction: Transaction,
                         request: DynamicRequest,
                         existingContent: Option[Content]): Content = existingContent match {
    case None ⇒
      Content(documentUri, itemSegment, newTransaction.revision,
        transactionList = List(newTransaction.uuid),
        body = Some(filterNulls(request.body).serializeToString()),
        isDeleted = false,
        createdAt = new Date(),
        modifiedAt = None
      )

    case Some(content) ⇒
      Content(documentUri, itemSegment, newTransaction.revision,
        transactionList = newTransaction.uuid +: content.transactionList,
        body = Some(filterNulls(request.body).serializeToString()),
        isDeleted = false,
        createdAt = content.createdAt,
        modifiedAt = Some(new Date())
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
                             existingContent: Option[Content]): Future[Transaction] = {
    val newTransaction = createNewTransaction(request, existingContent)
    val newContent = updateContent(documentUri, itemSegment, newTransaction, request, existingContent)
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
}
