package eu.inn.revault

import java.util.UUID

import akka.actor.{ActorLogging, ActorRef, Actor}
import com.datastax.driver.core.utils.UUIDs
import eu.inn.hyperbus.HyperBus
import eu.inn.hyperbus.model.DynamicRequest
import eu.inn.revault.db.{Monitor, Content, Db}
import eu.inn.revault.sharding.{ShardTaskComplete, ShardTask}
import akka.pattern.pipe
import scala.concurrent.duration._

import scala.concurrent.{Await, Future}
import scala.util.control.NonFatal

@SerialVersionUID(1L) case class RevaultCompleterShardTask(key: String, ttl: Long, path: String) extends ShardTask {
  def isExpired = ttl < System.currentTimeMillis()
  def group = "revault-completer"
}

@SerialVersionUID(1L) case class RevaultCompleterTaskResult(path: String, monitors: List[UUID])
@SerialVersionUID(1L) case class NoSuchResourceException(path: String) extends RuntimeException(s"No such resource: $path")
@SerialVersionUID(1L) case class IncorrectDataException(path: String, reason: String) extends RuntimeException(s"Data for $path is incorrect: $reason")

class RevaultCompleter(hyperBus: HyperBus, db: Db) extends Actor with ActorLogging {
  import context._
  import ContentLogic._

  override def receive: Receive = {
    case task: RevaultCompleterShardTask ⇒
      executeTask(sender(), task)
  }

  def executeTask(owner: ActorRef, task: RevaultCompleterShardTask): Unit = {
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

  def completeMonitors(task: RevaultCompleterShardTask, content: Content): Future[ShardTaskComplete] = {
    if (content.monitorList.isEmpty) {
      throw new IncorrectDataException(content.uri, "empty monitor list")
    }
    else {
      val monitorStream = content.monitorList.zipWithIndex.toStream map { case (monitorUuid, index) ⇒
        val monitorChannel = content.monitorChannel
        val monitorDtQuantum = MonitorLogic.getDtQuantum(UUIDs.unixTimestamp(monitorUuid))
        val monitorRevision = content.revision - index
        // todo: find a way to implement this without Await.result
        Await.result(
          db.selectMonitor(monitorDtQuantum, monitorChannel, content.uri, monitorRevision, monitorUuid) map {
            case None ⇒
              throw new IncorrectDataException(content.uri, s"monitor not found: $monitorDtQuantum/$monitorRevision/$monitorUuid")
            case Some(monitor) ⇒
              monitor
          },
        20.seconds) // todo: move timeout to configuration
      }

      // todo: test that selectMonitor is executed lazily
      val incompleteMonitors = monitorStream.takeWhile(_.completedAt.isEmpty).toVector.reverse
      val updateMonitors = incompleteMonitors.toStream.map { monitor ⇒
        val event = DynamicRequest(monitor.body)
        // todo: update event format
        hyperBus <| event flatMap { publishResult ⇒
          if (log.isDebugEnabled) {
            log.debug(s"Event $event is published with result $publishResult")
          }
          db.completeMonitor(monitor) map { _ ⇒
            if (log.isDebugEnabled) {
              log.debug(s"$monitor is complete")
            }
            monitor
          }
        }
      }

      Future.sequence(updateMonitors) map { monitors ⇒
        ShardTaskComplete(task, monitors.map(_.uuid))
      }
    }
  }
}
