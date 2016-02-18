package eu.inn.revault

import java.util.UUID

import akka.actor.{ActorLogging, ActorRef, Actor}
import com.datastax.driver.core.utils.UUIDs
import eu.inn.hyperbus.HyperBus
import eu.inn.hyperbus.model.DynamicRequest
import eu.inn.hyperbus.transport.api.PublishResult
import eu.inn.revault.db.{Monitor, Content, Db}
import eu.inn.revault.sharding.{ShardTaskComplete, ShardTask}
import akka.pattern.pipe
import scala.collection.mutable
import scala.concurrent.duration._

import scala.concurrent.{Await, Future}
import scala.util.control.NonFatal

@SerialVersionUID(1L) case class RevaultCompleterTask(key: String, ttl: Long, path: String) extends ShardTask {
  def isExpired = ttl < System.currentTimeMillis()
  def group = "revault-completer"
}

@SerialVersionUID(1L) case class RevaultCompleterTaskResult(path: String, monitors: List[UUID])
@SerialVersionUID(1L) case class NoSuchResourceException(path: String) extends RuntimeException(s"No such resource: $path")
@SerialVersionUID(1L) case class IncorrectDataException(path: String, reason: String) extends RuntimeException(s"Data for $path is incorrect: $reason")
@SerialVersionUID(1L) case class PublishFailedException(path: String, reason: String) extends RuntimeException(s"Publish for $path is failed: $reason")

class RevaultCompleter(hyperBus: HyperBus, db: Db) extends Actor with ActorLogging {
  import context._
  import ContentLogic._

  override def receive: Receive = {
    case task: RevaultCompleterTask ⇒
      executeTask(sender(), task)
  }

  def executeTask(owner: ActorRef, task: RevaultCompleterTask): Unit = {
    val (documentUri, itemSegment) = splitPath(task.path)
    db.selectContent(documentUri, itemSegment) flatMap {
      case None ⇒
        log.error(s"Didn't found resource to complete, dismissing task: $task")
        Future(ShardTaskComplete(task, new NoSuchResourceException(task.path)))
      case Some(content) ⇒
        try {
          completeMonitors(task, content)
        } catch {
          case NonFatal(e) ⇒
            log.error(e, s"Task $task didn't complete")
            Future(ShardTaskComplete(task, e))
        }
    } pipeTo owner
  }

  def completeMonitors(task: RevaultCompleterTask, content: Content): Future[ShardTaskComplete] = {
    if (content.monitorList.isEmpty) {
      throw new IncorrectDataException(content.uri, "empty monitor list")
    }
    else {
      selectIncompleteMonitors(content) map { incompleteMonitors ⇒
        try {
          val updateMonitors = incompleteMonitors.map { monitor ⇒
            val event = DynamicRequest(monitor.body)
            // todo: update event format
            // todo: idiomatic way without Await
            val f: Future[PublishResult] = hyperBus <| event
            val fmon: Future[Monitor] = f flatMap { publishResult ⇒
              if (log.isDebugEnabled) {
                log.debug(s"Event $event is published with result $publishResult")
              }
              // todo: other exception if complete fails
              db.completeMonitor(monitor) map { _ ⇒
                if (log.isDebugEnabled) {
                  log.debug(s"$monitor is complete")
                }
                monitor
              }
            }

            Await.result(fmon, 20.seconds) // todo: move timeout to configuration
          }

          ShardTaskComplete(task, RevaultCompleterTaskResult(task.path, updateMonitors.map(_.uuid)))
        } catch {
          case NonFatal(e) ⇒
            ShardTaskComplete(task, PublishFailedException(task.path, e.toString))
        }
      }
    }
  }

  // todo: implement in idiomatic way (need takeWhile for futures)
  def selectIncompleteMonitors(content: Content): Future[List[Monitor]] = Future {
    var stopSelecting = false
    content.monitorList flatMap { monitorUuid ⇒
      val monitorChannel = content.monitorChannel
      val monitorDtQuantum = MonitorLogic.getDtQuantum(UUIDs.unixTimestamp(monitorUuid))
      if (stopSelecting)
        None
      else {
        val select = db.selectMonitor(monitorDtQuantum, monitorChannel, content.uri, monitorUuid) map {
          case None ⇒
            throw new IncorrectDataException(content.uri, s"monitor not found: $monitorDtQuantum/$monitorUuid")
          case Some(monitor) ⇒
            if (monitor.completedAt.isEmpty)
              Some(monitor)
            else {
              stopSelecting = true
              None
            }
        }
        Await.result(select, 20.seconds) // todo: move timeout to configuration
      }
    } reverse
  }
}
