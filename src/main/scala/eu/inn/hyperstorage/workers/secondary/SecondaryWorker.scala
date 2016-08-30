package eu.inn.hyperstorage.workers.secondary

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.pipe
import eu.inn.hyperbus.Hyperbus
import eu.inn.hyperbus.model._
import eu.inn.hyperstorage.db._
import eu.inn.hyperstorage.sharding.{ShardTask, ShardTaskComplete}
import eu.inn.metrics.MetricsTracker

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

// todo: do we really need a ShardTaskComplete ?

trait SecondaryTaskTrait extends ShardTask {
  def ttl: Long

  def isExpired = ttl < System.currentTimeMillis()

  def group = "hyper-storage-secondary-worker"
}

@SerialVersionUID(1L) case class SecondaryTaskFailed(key: String, reason: String) extends RuntimeException(s"Secondary task for '$key' is failed with reason $reason")

class SecondaryWorker(val hyperbus: Hyperbus, val db: Db, val tracker: MetricsTracker, val indexManager: ActorRef) extends Actor with ActorLogging
  with BackgroundContentTaskCompleter
  with IndexDefTaskWorker
  with IndexContentTaskWorker {

  override def executionContext: ExecutionContext = context.dispatcher // todo: use other instead of this?
  import context.dispatcher

  override def receive: Receive = {
    case task: BackgroundContentTask ⇒
      val owner = sender()
      executeBackgroundTask(owner, task) recover withSecondaryTaskFailed(task) pipeTo owner

    case task: IndexDefTask ⇒
      val owner = sender()
      executeIndexDefTask(task) recover withSecondaryTaskFailed(task) pipeTo owner

    case task: IndexContentTask ⇒
      val owner = sender()
      indexNextBucket(task) recover withSecondaryTaskFailed(task) pipeTo owner
  }


  private def withSecondaryTaskFailed(task: SecondaryTaskTrait): PartialFunction[Throwable, ShardTaskComplete] = {
    case NonFatal(e) ⇒
      log.error(e, s"Can't execute $task")
      ShardTaskComplete(task, SecondaryTaskFailed(task.key, e.toString))
  }
}

object SecondaryWorker {
  def props(hyperbus: Hyperbus, db: Db, tracker: MetricsTracker, indexManager: ActorRef) = Props(classOf[SecondaryWorker],
    hyperbus, db, tracker, indexManager
  )
}

