package eu.inn.revault

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.Date

import akka.actor.{ActorLogging, ActorRef, Actor}
import eu.inn.hyperbus.HyperBus
import eu.inn.hyperbus.model.standard._
import eu.inn.hyperbus.model.{MessagingContextFactory, MessagingContext, Body, Message}
import eu.inn.hyperbus.serialization.MessageDeserializer
import eu.inn.hyperbus.transport.api.uri.Uri
import eu.inn.revault.db.{Content, Monitor, Db}
import eu.inn.revault.protocol.RevaultPut

import scala.concurrent.Future
import scala.util.control.NonFatal

// todo: rename PutTask
// todo: rename client
@SerialVersionUID(1L) case class RevaultTask(key: String, ttl: Long, client: ActorRef, content: String) extends Task {
  def isExpired = ttl < System.currentTimeMillis()
}

@SerialVersionUID(1L) case class RevaultTaskResult(content: String)

trait WorkerMessage
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

  private def createNewContent(prefix: String,
                               lastSegment: String,
                               newMonitor: Monitor,
                               request: RevaultPut,
                               existingContent: Option[Content]): Content =
    existingContent match {
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
          createdAt = new Date(),
          modifiedAt = None
        )
    }

  private def createAndInsertMonitorAndContent(prefix: String,
                                               lastSegment: String,
                                               request: RevaultPut,
                                               existingContent: Option[Content]): Future[Monitor] = {
    val newMonitor = createNewMonitor(request, existingContent)
    val newContent = createNewContent(prefix, lastSegment, newMonitor, request, existingContent)
    db.insertMonitor(newMonitor) flatMap { _ ⇒
      db.insertContent(newContent) map { _ ⇒
        newMonitor
      }
    }
  }

  def deserializeRequest(content: String): RevaultPut = {
    val byteStream = new ByteArrayInputStream(content.getBytes("UTF-8"))
    MessageDeserializer.deserializeRequestWith(byteStream) { (requestHeader, requestBodyJson) =>
      val msgId = requestHeader.messageId
      val cId = requestHeader.correlationId.getOrElse(msgId)
      requestHeader.method match { // todo: move this match to hyperbus and make a universal solution
        //case Method.GET => DynamicGet(requestHeader.uri, b, msgId, cId)
        //case Method.POST => DynamicPost(requestHeader.uri, b, msgId, cId)
        case Method.PUT => RevaultPut.deserializer(requestHeader, requestBodyJson)
        //case Method.DELETE => DynamicDelete(requestHeader.uri, b, msgId, cId)
        //case Method.PATCH => DynamicPatch(requestHeader.uri, b, msgId, cId)
      }
    }
  }

  def executeTask(owner: ActorRef, task: RevaultTask): Unit = {
    import akka.pattern.pipe

    val (prefix: String, lastSegment: String, hyperBusRequest: RevaultPut) = try {
      val hyperBusRequest = deserializeRequest(task.content)
      val (prefix, lastSegment) = splitPath(hyperBusRequest.path)
      (prefix, lastSegment, hyperBusRequest)
    }
    catch {
      case NonFatal(e) ⇒
        log.error(e, s"Can't deserialize and split path for: $task")
        ("","")
    }
    if (prefix == "") {
      owner ! ReadyForNextTask
    }
    else {
      become(taskWaitResult(owner, task)(hyperBusRequest))

      // fetch and complete existing content
      val futureExistingContent: Future[Option[Content]] = selectAndCompletePrevious(prefix, lastSegment)

      val futureMonitor: Future[Monitor] = futureExistingContent flatMap { existingContent ⇒
        createAndInsertMonitorAndContent(prefix, lastSegment, hyperBusRequest, existingContent)
      }

      futureMonitor flatMap { newMonitor ⇒
        // todo: add publish event and other actions here

        db.completeMonitor(newMonitor) map { _ ⇒
          TaskComplete(task, newMonitor)
        } recover {
          case NonFatal(e) ⇒
            TaskAccepted(task, newMonitor, e)
        }
      } recover {
        case NonFatal(e) ⇒
          TaskFailed(task,e)
      } pipeTo context.self
    }
  }

  private def createNewMonitor(request: RevaultPut, existingContent: Option[Content]): Monitor = existingContent match {
    case None ⇒
      MonitorLogic.newMonitor(request.path, 1, request.serializeToString())
    case Some(content) ⇒
      MonitorLogic.newMonitor(request.path, content.revision + 1, request.serializeToString())
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
            else
              completePreviousTask(content,monitor)
        }
    }
  }

  private def completePreviousTask(content: Content, monitor: Monitor): Future[Option[Content]] = ??? // todo: implement

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
      log.error(e, s"Task $task is failed")
      owner ! ReadyForNextTask
      val resultContent = InternalServerError(ErrorBody("update_failed",Some(e.toString))).serializeToString()
      put.client ! RevaultTaskResult(resultContent)
      unbecome()
  }

  // todo: describe uri to resource/collection item matching
  private def splitPath(path: String): (String,String) = {
    // todo: implement collections
    (path,"")
  }
}

/*

1. Check existing resource monitor
2. complete if previous update is not complete
3. create & insert new monitor
4. update resource
5. send accepted to the client (if any)
6. publish event
  6.1. + revision
  6.2. + :events path
  6.3. + for post request add self link
7. when event is published complete monitor
8. request next task


plan:
  + monitor body content
  + methods
  + pipeTo logic
  + kafka event generator
  + get method
  + test put + get
  * other methods
  * collections
  *

*/
