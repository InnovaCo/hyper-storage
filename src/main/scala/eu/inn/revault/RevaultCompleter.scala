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
import scala.collection.{mutable, Seq}
import scala.collection.mutable.Builder
import scala.concurrent.duration._

import scala.concurrent.{ExecutionContext, Promise, Await, Future}
import scala.util.control.NonFatal

@SerialVersionUID(1L) case class RevaultCompleterTask(key: String, ttl: Long, path: String) extends ShardTask {
  def isExpired = ttl < System.currentTimeMillis()
  def group = "revault-completer"
}

@SerialVersionUID(1L) case class RevaultCompleterTaskResult(path: String, monitors: Seq[UUID])
@SerialVersionUID(1L) case class NoSuchResourceException(path: String) extends RuntimeException(s"No such resource: $path")
@SerialVersionUID(1L) case class IncorrectDataException(path: String, reason: String) extends RuntimeException(s"Data for $path is incorrect: $reason")
@SerialVersionUID(1L) case class CompletionFailedException(path: String, reason: String) extends RuntimeException(s"Complete for $path is failed: $reason")

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
      selectIncompleteMonitors(content) flatMap { incompleteMonitors ⇒
        FutureUtils.serial(incompleteMonitors) { monitor ⇒
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
        } map { updatedMonitors ⇒
          ShardTaskComplete(task, RevaultCompleterTaskResult(task.path, updatedMonitors.map(_.uuid)))
        } recover {
          case NonFatal(e) ⇒
            ShardTaskComplete(task, CompletionFailedException(task.path, e.toString))
        }
      }
    }
  }

  def selectIncompleteMonitors(content: Content): Future[Seq[Monitor]] = {
    val monitorsFStream = content.monitorList.toStream.map { monitorUuid ⇒
      val monitorChannel = content.monitorChannel
      val monitorDtQuantum = MonitorLogic.getDtQuantum(UUIDs.unixTimestamp(monitorUuid))
      db.selectMonitor(monitorDtQuantum, monitorChannel, content.uri, monitorUuid)
    }
    FutureUtils.takeUntilNone(monitorsFStream) map (_.reverse)
  }
}

object FutureUtils {
  def takeUntilNone[T](iterable: Iterable[Future[Option[T]]])(implicit ec: ExecutionContext): Future[Seq[T]] = {
    val iterator = iterable.iterator
    takeUntilNone(Seq.newBuilder[T],
      if (iterator.hasNext) iterator.next() else Future.successful(None)
    ) map {
      _.result()
    }
  }

  private def takeUntilNone[T](builder: mutable.Builder[T, Seq[T]], f: ⇒ Future[Option[T]])
                              (implicit ec: ExecutionContext): Future[mutable.Builder[T, Seq[T]]] = {
    f flatMap {
      case None ⇒ Future.successful(builder)
      case Some(t) ⇒ takeUntilNone(builder += t, f)
    }
  }

  def serial[A, B](in: Seq[A])(f: A ⇒ Future[B])(implicit ec: ExecutionContext): Future[Seq[B]] =
    in.foldLeft(Future.successful(Seq.newBuilder[B])) { case (fr, a) ⇒
      for (result ← fr; r ← f(a)) yield result += r
    } map (_.result())
}
