package eu.inn.revault

import java.util.Date

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.fasterxml.jackson.core.JsonParser
import com.oracle.webservices.internal.api.message.MessageContextFactory
import eu.inn.binders.dynamic._
import eu.inn.hyperbus.HyperBus
import eu.inn.hyperbus.model._
import eu.inn.hyperbus.model.annotations.request
import eu.inn.hyperbus.model.standard._
import eu.inn.hyperbus.transport.api.uri.{SpecificValue, UriPart, Uri}
import eu.inn.hyperbus.util.StringSerializer
import eu.inn.revault.db.{Content, Db, Monitor}
import akka.pattern.pipe
import scala.concurrent.Future
import scala.util.Try
import scala.util.control.NonFatal

// todo: rename PutTask
// todo: rename client
@SerialVersionUID(1L) case class RevaultTask(key: String, ttl: Long, content: String) extends Task {
  def isExpired = ttl < System.currentTimeMillis()
}

@SerialVersionUID(1L) case class RevaultTaskResult(content: String)

sealed trait WorkerMessage // todo: rename
case class TaskComplete(task: Task, monitor: Monitor) extends WorkerMessage
case class TaskFailed(task: Task, inner: Throwable) extends WorkerMessage
case class TaskAccepted(task: Task, monitor: Monitor, inner: Throwable) extends WorkerMessage

// this sent to recovery actor
// todo: rename
case class RevaultTaskAccepted(monitor: db.Monitor)

@request("fix:{path:*}")
case class RevaultFix(path: String, body: EmptyBody) extends StaticPost(body)

// todo: rename WorkerActor
class WorkerActor(hyperBus: HyperBus, db: Db, recoveryActor: ActorRef) extends Actor with ActorLogging {
  import context._

  def receive = {
    case task: RevaultTask ⇒
      executeTask(sender(), task)
  }

  def executeTask(owner: ActorRef, task: RevaultTask): Unit = {
    Try{
      val request = DynamicRequest(task.content)
      val (prefix, lastSegment) = splitPath(request.path)
      (prefix, lastSegment, request)
    } map {
      case (prefix: String, lastSegment: String, request: DynamicRequest) ⇒
        become(taskWaitResult(owner, task)(request))

        // fetch and complete existing content
        if (request.uri.pattern == RevaultFix.uriPattern) {
          executeRecourceFixTask(prefix, lastSegment, task, request)
        }
        else {
          executeResourceUpdateTask(prefix, lastSegment, task, request)
        }
    } recover {
      case NonFatal(e) ⇒
        log.error(e, s"Can't deserialize and split path for: $task")
        owner ! WorkerTaskComplete(Some(hyperbusException(e, task)))
    }
  }

  def executeResourceUpdateTask(prefix: String, lastSegment: String, task: RevaultTask, request: DynamicRequest) = {
    val futureUpdateContent = selectAndCompletePrevious(prefix, lastSegment) flatMap {
      case (existingContent, _) ⇒
        updateResource(prefix, lastSegment, request, existingContent)
    }

    futureUpdateContent flatMap { newMonitor ⇒
      completeTask(newMonitor) map { _ ⇒
        TaskComplete(task, newMonitor)
      } recover {
        case NonFatal(e) ⇒
          TaskAccepted(task, newMonitor, e)
      }
    } recover {
      case NonFatal(e) ⇒
        TaskFailed(task, e)
    } pipeTo context.self
  }

  private def executeRecourceFixTask(prefix: String, lastSegment: String, task: RevaultTask, request: DynamicRequest): Unit = {
    val futureExistingContent = selectAndCompletePrevious(prefix, lastSegment)
    futureExistingContent map {
      case (_, Some(previousMonitor)) ⇒
        TaskComplete(task, previousMonitor)
      case (Some(existing), None) ⇒ {
        // todo: {} is not correct body of monoitor here.
        val mon = Monitor(existing.monitorDt, existing.monitorChannel, request.path, existing.revision, "{}",
          Some(existing.modifiedAt.getOrElse(existing.createdAt))
        )
        TaskComplete(task, mon)
      }
      case _ ⇒ {
          implicit val mcx: MessagingContextFactory = request
          val error = NotFound(ErrorBody("not_found", Some(s"Resource '${request.path}' is not found")))
          TaskFailed(task, error)
        }
    } recover {
      case NonFatal(e) ⇒
        TaskFailed(task, e)
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

  private def selectAndCompletePrevious(prefix: String, lastSegment: String): Future[(Option[Content], Option[Monitor])] = {
    db.selectContent(prefix, lastSegment) flatMap {
      case None ⇒ Future((None, None))
      case Some(content) ⇒
        db.selectMonitor(content.monitorDt, content.monitorChannel, prefix + "/" + lastSegment) flatMap {
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
    }
  }

  private def taskWaitResult(owner: ActorRef, originalTask: RevaultTask)(implicit mcf: MessagingContextFactory): Receive = {
    case TaskComplete(task, monitor) if task == originalTask ⇒
      if (log.isDebugEnabled) {
        log.debug(s"Task $originalTask is completed")
      }
      val monId = monitor.path + ":" + monitor.revision
      val resultContent = Ok(protocol.Monitor(monId, "complete", monitor.completedAt)).serializeToString()
      owner ! WorkerTaskComplete(Some(RevaultTaskResult(resultContent)))
      unbecome()

    case TaskAccepted(task, monitor, e) if task == originalTask ⇒
      log.warning(s"Task $originalTask was accepted but didn't yet complete because of $e")
      recoveryActor ! RevaultTaskAccepted(monitor)
      val monId = monitor.path + ":" + monitor.revision
      val resultContent = Accepted(protocol.Monitor(monId, "in-progress", None)).serializeToString()
      owner ! WorkerTaskComplete(Some(RevaultTaskResult(resultContent)))
      unbecome()

    case TaskFailed(task, e) if task == originalTask ⇒
      owner ! WorkerTaskComplete(Some(hyperbusException(e, task)))
      unbecome()
  }

  private def hyperbusException(e: Throwable, task: Task): RevaultTaskResult = {
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

  private def updateContent(prefix: String,
                            lastSegment: String,
                            newMonitor: Monitor,
                            request: DynamicRequest,
                            existingContent: Option[Content]): Content =
  request.method match {
    case Method.PUT ⇒ putContent(prefix, lastSegment, newMonitor, request, existingContent)
    case Method.PATCH ⇒ patchContent(prefix, lastSegment, newMonitor, request, existingContent)
    case Method.DELETE ⇒ deleteContent(prefix, lastSegment, newMonitor, request, existingContent)
  }

  private def putContent(prefix: String,
                            lastSegment: String,
                            newMonitor: Monitor,
                            request: DynamicRequest,
                            existingContent: Option[Content]): Content = existingContent match {
    case None ⇒
      Content(prefix, lastSegment, newMonitor.revision,
        monitorDt = newMonitor.dt, monitorChannel = newMonitor.channel,
        body = Some(filterNulls(request.body).serializeToString()),
        isDeleted = false,
        createdAt = new Date(),
        modifiedAt = None
      )

    case Some(content) ⇒
      Content(prefix, lastSegment, newMonitor.revision,
        monitorDt = newMonitor.dt, monitorChannel = newMonitor.channel,
        body = Some(filterNulls(request.body).serializeToString()),
        isDeleted = false,
        createdAt = content.createdAt,
        modifiedAt = Some(new Date())
      )
  }

  private def patchContent(prefix: String,
                         lastSegment: String,
                         newMonitor: Monitor,
                         request: DynamicRequest,
                         existingContent: Option[Content]): Content = existingContent match {
    case None ⇒ {
      implicit val mcx = request
      throw NotFound(ErrorBody("not_found", Some(s"Resource '${request.path}' is not found")))
    }

    case Some(content) ⇒
      Content(prefix, lastSegment, newMonitor.revision,
        monitorDt = newMonitor.dt, monitorChannel = newMonitor.channel,
        body = Some(mergeBody(deserializeBody(content.body), request.body).serializeToString()),
        isDeleted = false,
        createdAt = content.createdAt,
        modifiedAt = Some(new Date())
      )
  }

  private def deleteContent(prefix: String,
                           lastSegment: String,
                           newMonitor: Monitor,
                           request: DynamicRequest,
                           existingContent: Option[Content]): Content = existingContent match {
    case None ⇒ {
      implicit val mcx = request
      throw NotFound(ErrorBody("not_found", Some(s"Resource '${request.path}' is not found")))
    }

    case Some(content) ⇒
      Content(prefix, lastSegment, newMonitor.revision,
        monitorDt = newMonitor.dt, monitorChannel = newMonitor.channel,
        body = None,
        isDeleted = true,
        createdAt = content.createdAt,
        modifiedAt = Some(new Date())
      )
  }

  private def updateResource(prefix: String,
                             lastSegment: String,
                             request: DynamicRequest,
                             existingContent: Option[Content]): Future[Monitor] = {
    val newMonitor = createNewMonitor(request, existingContent)
    val newContent = updateContent(prefix, lastSegment, newMonitor, request, existingContent)
    db.insertMonitor(newMonitor) flatMap { _ ⇒
      db.insertContent(newContent) map { _ ⇒
        newMonitor
      }
    }
  }

  private def mergeBody(existing: DynamicBody, patch: DynamicBody): DynamicBody = {
    DynamicBody(filterNulls(existing.content.merge(patch.content)))
  }

  // todo: describe uri to resource/collection item matching
  private def splitPath(path: String): (String,String) = {
    // todo: implement collections
    (path,"")
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

  def deserializeBody(content: Option[String]) : DynamicBody = content match {
    case None ⇒ DynamicBody(Null)
    case Some(string) ⇒ {
      import eu.inn.binders._
      implicit val jsf = new eu.inn.hyperbus.serialization.JsonHalSerializerFactory[eu.inn.binders.naming.PlainConverter]
      val value = eu.inn.binders.json.SerializerFactory.findFactory().withStringParser(string) { case jsonParser ⇒
        import eu.inn.hyperbus.serialization.MessageSerializer.bindOptions // dont remove this!
        jsonParser.unbind[Value]
      }
      DynamicBody(value)
    }
  }
}
