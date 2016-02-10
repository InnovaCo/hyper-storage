package eu.inn.revault

import java.util.Date

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.fasterxml.jackson.core.JsonParser
import eu.inn.binders.dynamic.{Value, Null}
import eu.inn.hyperbus.HyperBus
import eu.inn.hyperbus.model._
import eu.inn.hyperbus.model.standard._
import eu.inn.hyperbus.util.StringSerializer
import eu.inn.revault.db.{Content, Db, Monitor}

import scala.concurrent.Future
import scala.util.Try
import scala.util.control.NonFatal

// todo: rename PutTask
// todo: rename client
@SerialVersionUID(1L) case class RevaultTask(key: String, ttl: Long, client: ActorRef, content: String) extends Task {
  def isExpired = ttl < System.currentTimeMillis()
}

@SerialVersionUID(1L) case class RevaultTaskResult(content: String)

trait WorkerMessage // todo: rename
case class TaskComplete(task: Task, monitor: Monitor) extends WorkerMessage
case class TaskFailed(task: Task, inner: Throwable) extends WorkerMessage
case class TaskAccepted(task: Task, monitor: Monitor, inner: Throwable) extends WorkerMessage

// todo: rename WorkerActor
class WorkerActor(hyperBus: HyperBus, db: Db) extends Actor with ActorLogging {
  import context._

  def receive = {
    case task: RevaultTask ⇒
      executeTask(sender(), task)
  }

  def executeTask(owner: ActorRef, task: RevaultTask): Unit = {
    import akka.pattern.pipe

    Try{
      val request = DynamicRequest(task.content)
      val (prefix, lastSegment) = splitPath(request.path)
      (prefix, lastSegment, request)
    } map {
      case (prefix: String, lastSegment: String, request: DynamicRequest) ⇒
        become(taskWaitResult(owner, task)(request))

        // fetch and complete existing content
        val futureExistingContent = selectAndCompletePrevious(prefix, lastSegment)

        val futureUpdateContent = futureExistingContent flatMap { existingContent ⇒
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
    } recover {
      case NonFatal(e) ⇒
        log.error(e, s"Can't deserialize and split path for: $task")
        owner ! ReadyForNextTask
    }
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
      headers = request.headers + "hyperbus:revision" → Seq(revision.toString)
    ).serializeToString())
  }

  private def selectAndCompletePrevious(prefix: String, lastSegment: String): Future[Option[Content]] = {
    db.selectContent(prefix,lastSegment) flatMap {
      case None ⇒ Future(None)
      case Some(content) ⇒
        db.selectMonitor(content.monitorDt, content.monitorChannel, prefix + "/" + lastSegment) flatMap {
          case None ⇒ Future(Some(content)) // If no monitor, consider that it's complete
          case Some(monitor) ⇒
            if (monitor.completedAt.isDefined)
              Future(Some(content))
            else {
              completeTask(monitor) map { _ ⇒
                Some(content)
              }
            }
        }
    }
  }

  private def taskWaitResult(owner: ActorRef, put: RevaultTask)(implicit mcf: MessagingContextFactory): Receive = {
    case TaskComplete(task, monitor) if task == put ⇒
      if (log.isDebugEnabled) {
        log.debug(s"Task $put is completed")
      }
      owner ! ReadyForNextTask
      val monId = monitor.path + ":" + monitor.revision
      val resultContent = Ok(protocol.Monitor(monId, "complete", monitor.completedAt)).serializeToString()
      put.client ! RevaultTaskResult(resultContent)
      unbecome()

    case TaskAccepted(task, monitor, e) if task == put ⇒
      log.warning(s"Task was accepted but didn't yet complete because of $e")
      // todo: send message to complete task
      val monId = monitor.path + ":" + monitor.revision
      val resultContent = Accepted(protocol.Monitor(monId, "in-progress", None)).serializeToString()
      put.client ! RevaultTaskResult(resultContent)
      owner ! ReadyForNextTask
      unbecome()

    case TaskFailed(task, e) if task == put ⇒
      owner ! ReadyForNextTask

      val (response:HyperBusException[ErrorBody],logException) = e match {
        case h: NotFound[ErrorBody] ⇒ (h, false)
        case h: HyperBusException[ErrorBody] ⇒ (h, true)
        case other ⇒ (InternalServerError(ErrorBody("update_failed",Some(e.toString))), true)
      }

      if (logException) {
        log.error(e, s"Task $task is failed")
      }

      val resultContent = response.serializeToString()
      put.client ! RevaultTaskResult(resultContent)
      unbecome()
  }

  private def updateContent(prefix: String,
                            lastSegment: String,
                            newMonitor: Monitor,
                            request: DynamicRequest,
                            existingContent: Option[Content]): Content =
  request.method match {
    case Method.PUT ⇒ putContent(prefix, lastSegment, newMonitor, request, existingContent)
    case Method.PATCH ⇒ patchContent(prefix, lastSegment, newMonitor, request, existingContent)
  }

  private def putContent(prefix: String,
                            lastSegment: String,
                            newMonitor: Monitor,
                            request: DynamicRequest,
                            existingContent: Option[Content]): Content = existingContent match {
    case None ⇒
      Content(prefix, lastSegment, newMonitor.revision,
        monitorDt = newMonitor.dt, monitorChannel = newMonitor.channel,
        body = Some(request.body.serializeToString()),
        isDeleted = false,
        createdAt = new Date(),
        modifiedAt = None
      )

    case Some(content) ⇒
      Content(prefix, lastSegment, newMonitor.revision,
        monitorDt = newMonitor.dt, monitorChannel = newMonitor.channel,
        body = Some(request.body.serializeToString()),
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

  private def mergeBody(existing: DynamicBody, patch: DynamicBody): DynamicBody = ???

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


/*
private[revault] object SerializerMap {
  val deserializers: Map[(Boolean,String),(RequestHeader, JsonParser) => RevaultRequest] = Map(
    (false,Method.PUT) → RevaultPut.deserializer,
    (true,Method.PUT) → RevaultFeedPut.deserializer,
    (false,Method.PATCH) → RevaultPatch.deserializer,
    (true,Method.PATCH) → RevaultFeedPatch.deserializer
  )
}
*/
