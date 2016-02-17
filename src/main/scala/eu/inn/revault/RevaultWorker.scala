package eu.inn.revault

import java.util.Date

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.datastax.driver.core.utils.UUIDs
import com.fasterxml.jackson.core.JsonParser
import com.oracle.webservices.internal.api.message.MessageContextFactory
import eu.inn.binders.dynamic._
import eu.inn.hyperbus.HyperBus
import eu.inn.hyperbus.model._
import eu.inn.hyperbus.model.annotations.request
import eu.inn.hyperbus.model.serialization.util.StringDeserializer
import eu.inn.hyperbus.model.standard._
import eu.inn.hyperbus.transport.api.uri.{SpecificValue, UriPart, Uri}
import eu.inn.hyperbus.util.StringSerializer
import eu.inn.revault.db.{Content, Db, Monitor}
import akka.pattern.pipe
import eu.inn.revault.sharding.{ShardTaskComplete, ShardTask}
import scala.concurrent.Future
import scala.util.Try
import scala.util.control.NonFatal

@SerialVersionUID(1L) case class RevaultShardTask(key: String, ttl: Long, content: String) extends ShardTask {
  def isExpired = ttl < System.currentTimeMillis()
  def group = "revault"
}

@SerialVersionUID(1L) case class RevaultTaskResult(content: String)

case class RevaultWorkerTaskComplete(task: ShardTask, monitor: Monitor)
case class RevaultWorkerTaskFailed(task: ShardTask, inner: Throwable)
case class RevaultWorkerTaskAccepted(task: ShardTask, monitor: Monitor, inner: Throwable)

// this is sent to recovery actor when task was accepted but failed to complete
case class RevaultTaskIncomplete(monitor: db.Monitor)

// this is sent from recovery actor when it detects that there is incomplete task
@request("fix:{path:*}")
case class RevaultFix(path: String, body: EmptyBody) extends StaticPost(body)

class RevaultWorker(hyperBus: HyperBus, db: Db, recoveryActor: ActorRef) extends Actor with ActorLogging {
  import context._
  import ContentLogic._

  def receive = {
    case task: RevaultShardTask ⇒
      executeTask(sender(), task)
  }

  def executeTask(owner: ActorRef, task: RevaultShardTask): Unit = {
    Try{
      val request = DynamicRequest(task.content)
      val (documentUri, itemSegment) = splitPath(request.path)
      (documentUri, itemSegment, request)
    } map {
      case (documentUri: String, itemSegment: String, request: DynamicRequest) ⇒
        become(taskWaitResult(owner, task)(request))

        // fetch and complete existing content
        if (request.uri.pattern == RevaultFix.uriPattern) {
          executeResourceFixTask(documentUri, itemSegment, task, request)
        }
        else {
          executeResourceUpdateTask(documentUri, itemSegment, task, request)
        }
    } recover {
      case NonFatal(e) ⇒
        log.error(e, s"Can't deserialize and split path for: $task")
        owner ! ShardTaskComplete(task, hyperbusException(e, task))
    }
  }

  def executeResourceUpdateTask(documentUri: String, itemSegment: String, task: RevaultShardTask, request: DynamicRequest) = {
    val futureUpdateContent = selectAndCompletePrevious(documentUri, itemSegment) flatMap {
      case (existingContent, _) ⇒
        updateResource(documentUri, itemSegment, request, existingContent)
    }

    futureUpdateContent flatMap { newMonitor ⇒
      completeTask(newMonitor) map { _ ⇒
        RevaultWorkerTaskComplete(task, newMonitor)
      } recover {
        case NonFatal(e) ⇒
          RevaultWorkerTaskAccepted(task, newMonitor, e)
      }
    } recover {
      case NonFatal(e) ⇒
        RevaultWorkerTaskFailed(task, e)
    } pipeTo context.self
  }

  private def executeResourceFixTask(documentUri: String, itemSegment: String, task: RevaultShardTask, request: DynamicRequest): Unit = {
    val futureExistingContent = selectAndCompletePrevious(documentUri, itemSegment)
    futureExistingContent map {
      case (_, Some(previousMonitor)) ⇒
        RevaultWorkerTaskComplete(task, previousMonitor)
      case (Some(existing), None) ⇒ {
        // todo: fake monitor here may lead to problems

        val monitorUuid = existing.monitorList.headOption.getOrElse(UUIDs.endOf(
          existing.modifiedAt.getOrElse(existing.createdAt).getTime
        ))
        val monitorChannel = MonitorLogic.channelFromUri(existing.uri)
        val monitorDtQuantum = MonitorLogic.getDtQuantum(UUIDs.unixTimestamp(monitorUuid))

        val mon = Monitor(monitorDtQuantum, monitorChannel, request.path, existing.revision,
          monitorUuid, "{}",
          Some(existing.modifiedAt.getOrElse(existing.createdAt))
        )
        RevaultWorkerTaskComplete(task, mon)
      }
      case _ ⇒ {
          implicit val mcx: MessagingContextFactory = request
          val error = NotFound(ErrorBody("not_found", Some(s"Resource '${request.path}' is not found")))
          RevaultWorkerTaskFailed(task, error)
        }
    } recover {
      case NonFatal(e) ⇒
        RevaultWorkerTaskFailed(task, e)
    } pipeTo context.self
  }

  private def completeTask(monitor: Monitor): Future[Unit] = {
    // todo: protect from corrupt monitor body!
    val event = DynamicRequest(monitor.body)
    hyperBus <| event flatMap { publishResult ⇒
      if (log.isDebugEnabled) {
        log.debug(s"Event $event is published with result $publishResult")
      }
      db.completeMonitor(monitor)
    }
  }

  private def createNewMonitor(request: DynamicRequest, existingContent: Option[Content]): Monitor = {
    val revision = existingContent match {
      case None ⇒ 1
      case Some(content) ⇒ content.revision + 1
    }
    MonitorLogic.newMonitor(request.path, revision, request.copy(
      uri = request.uri.copy(
        pattern = SpecificValue(request.uri.pattern.specific + "/feed")
      ),
      headers = request.headers + "hyperbus:revision" → Seq(revision.toString)
    ).serializeToString())
  }

  private def selectAndCompletePrevious(documentUri: String, itemSegment: String): Future[(Option[Content], Option[Monitor])] = {
    db.selectContent(documentUri, itemSegment) flatMap {
      case None ⇒ Future((None, None))
      case Some(content) ⇒
        content.monitorList.headOption map { monitorUuid ⇒
          val monitorDtQuantum = MonitorLogic.getDtQuantum(UUIDs.unixTimestamp(monitorUuid))
          db.selectMonitor(monitorDtQuantum,
            content.monitorChannel,
            content.uri,
            content.revision,
            monitorUuid) flatMap {
            case None ⇒ Future(Some(content), None) // If no monitor, consider that it's complete
            case Some(monitor) ⇒
              if (monitor.completedAt.isDefined)
                Future((Some(content), Some(monitor)))
              else {
                completeTask(monitor) map { _ ⇒
                  (Some(content), Some(monitor))
                }
              }
          }
        } getOrElse {
          Future((Some(content), None))
        }
    }
  }

  private def taskWaitResult(owner: ActorRef, originalTask: RevaultShardTask)(implicit mcf: MessagingContextFactory): Receive = {
    case RevaultWorkerTaskComplete(task, monitor) if task == originalTask ⇒
      if (log.isDebugEnabled) {
        log.debug(s"Task $originalTask is completed")
      }
      val monId = monitor.uri + ":" + monitor.revision
      val resultContent = Ok(protocol.Monitor(monId, "complete", monitor.completedAt)).serializeToString()
      owner ! ShardTaskComplete(task, RevaultTaskResult(resultContent))
      unbecome()

    case RevaultWorkerTaskAccepted(task, monitor, e) if task == originalTask ⇒
      log.warning(s"Task $originalTask was accepted but didn't yet complete because of $e")
      recoveryActor ! RevaultTaskIncomplete(monitor)
      val monId = monitor.uri + ":" + monitor.revision
      val resultContent = Accepted(protocol.Monitor(monId, "in-progress", None)).serializeToString()
      owner ! ShardTaskComplete(task, RevaultTaskResult(resultContent))
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
                            newMonitor: Monitor,
                            request: DynamicRequest,
                            existingContent: Option[Content]): Content =
  request.method match {
    case Method.PUT ⇒ putContent(documentUri, itemSegment, newMonitor, request, existingContent)
    case Method.PATCH ⇒ patchContent(documentUri, itemSegment, newMonitor, request, existingContent)
    case Method.DELETE ⇒ deleteContent(documentUri, itemSegment, newMonitor, request, existingContent)
  }

  private def putContent(documentUri: String,
                            itemSegment: String,
                            newMonitor: Monitor,
                            request: DynamicRequest,
                            existingContent: Option[Content]): Content = existingContent match {
    case None ⇒
      Content(documentUri, itemSegment, newMonitor.revision,
        monitorList = List(newMonitor.uuid),
        body = Some(filterNulls(request.body).serializeToString()),
        isDeleted = false,
        createdAt = new Date(),
        modifiedAt = None
      )

    case Some(content) ⇒
      Content(documentUri, itemSegment, newMonitor.revision,
        monitorList = newMonitor.uuid +: content.monitorList,
        body = Some(filterNulls(request.body).serializeToString()),
        isDeleted = false,
        createdAt = content.createdAt,
        modifiedAt = Some(new Date())
      )
  }

  private def patchContent(documentUri: String,
                         itemSegment: String,
                         newMonitor: Monitor,
                         request: DynamicRequest,
                         existingContent: Option[Content]): Content = existingContent match {
    case None ⇒ {
      implicit val mcx = request
      throw NotFound(ErrorBody("not_found", Some(s"Resource '${request.path}' is not found")))
    }

    case Some(content) ⇒
      Content(documentUri, itemSegment, newMonitor.revision,
        monitorList = newMonitor.uuid +: content.monitorList,
        body = Some(mergeBody(StringDeserializer.dynamicBody(content.body), request.body).serializeToString()),
        isDeleted = false,
        createdAt = content.createdAt,
        modifiedAt = Some(new Date())
      )
  }

  private def deleteContent(documentUri: String,
                           itemSegment: String,
                           newMonitor: Monitor,
                           request: DynamicRequest,
                           existingContent: Option[Content]): Content = existingContent match {
    case None ⇒ {
      implicit val mcx = request
      throw NotFound(ErrorBody("not_found", Some(s"Resource '${request.path}' is not found")))
    }

    case Some(content) ⇒
      Content(documentUri, itemSegment, newMonitor.revision,
        monitorList = newMonitor.uuid +: content.monitorList,
        body = None,
        isDeleted = true,
        createdAt = content.createdAt,
        modifiedAt = Some(new Date())
      )
  }

  private def updateResource(documentUri: String,
                             itemSegment: String,
                             request: DynamicRequest,
                             existingContent: Option[Content]): Future[Monitor] = {
    val newMonitor = createNewMonitor(request, existingContent)
    val newContent = updateContent(documentUri, itemSegment, newMonitor, request, existingContent)
    db.insertMonitor(newMonitor) flatMap { _ ⇒
      db.insertContent(newContent) map { _ ⇒
        newMonitor
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
