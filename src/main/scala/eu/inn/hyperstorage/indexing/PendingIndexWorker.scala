package eu.inn.hyperstorage.indexing

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import eu.inn.hyperbus.Hyperbus
import eu.inn.hyperstorage.{IndexTask, IndexTaskResult}
import eu.inn.hyperstorage.db.{Db, IndexDef, PendingIndex}
import eu.inn.metrics.MetricsTracker
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

case object StartPendingIndexWorker
case object CompletePendingIndex
case class BeginIndexing(indexDef: IndexDef, lastItemSegment: Option[String])
case class WaitForIndexDef(pendingIndex: PendingIndex)
case class IndexNextBatchTimeout(processId: Long)

// todo: add indexing progress log
class PendingIndexWorker(cluster: ActorRef, indexKey: IndexWorkersKey, hyperbus: Hyperbus, db: Db, tracker: MetricsTracker)
  extends Actor with ActorLogging {

  override def preStart(): Unit = {
    super.preStart()
    self ! StartPendingIndexWorker
  }

  override def receive = starOrStop orElse {
    case WaitForIndexDef ⇒
      import context._
      become(waitingForIndexDef)
      IndexWorkerImpl.selectPendingIndex(context.self, indexKey, db)
  }

  def starOrStop: Receive = {
    case StartPendingIndexWorker ⇒
      import context._
      IndexWorkerImpl.selectPendingIndex(context.self, indexKey, db)

    case CompletePendingIndex ⇒
      context.parent ! IndexingComplete(indexKey)
      context.stop(self)

    case BeginIndexing(indexDef, lastItemSegment) ⇒
      indexNextBatch(0, indexDef, lastItemSegment)
  }

  def waitingForIndexDef: Receive = starOrStop orElse {
    case WaitForIndexDef(pendingIndex) ⇒
      import context._
      IndexWorkerImpl.deletePendingIndex(context.self, pendingIndex, db)
  }

  def indexing(processId: Long, indexDef: IndexDef, lastItemSegment: Option[String]): Receive = {
    case IndexNextBatchTimeout(p) if p == processId ⇒
      indexNextBatch(processId + 1, indexDef, lastItemSegment)

    case IndexTaskResult(Some(newLastItemSegment), p) if p == processId ⇒
      indexNextBatch(processId + 1, indexDef, Some(newLastItemSegment))

    case IndexTaskResult(None, p) if p == processId ⇒
      context.parent ! IndexingComplete(indexKey)
      context.stop(self)
  }

  def indexNextBatch(processId: Long, indexDef: IndexDef, lastItemSegment: Option[String]): Unit = {
    import context.dispatcher
    context.become(indexing(processId, indexDef, lastItemSegment))
    cluster ! IndexTask(System.currentTimeMillis() + IndexWorkerImpl.RETRY_PERIOD.toMillis,
      indexDef, lastItemSegment, processId)
    context.system.scheduler.scheduleOnce(IndexWorkerImpl.RETRY_PERIOD*2, self, IndexNextBatchTimeout(processId))
  }
}

object PendingIndexWorker {
  def props(cluster: ActorRef, indexKey: IndexWorkersKey, hyperbus: Hyperbus, db: Db, tracker: MetricsTracker) = Props(
    classOf[PendingIndexWorker], cluster: ActorRef, indexKey, hyperbus, db, tracker
  )
}

private [indexing] object IndexWorkerImpl {
  val log = LoggerFactory.getLogger(getClass)
  import scala.concurrent.duration._
  val RETRY_PERIOD = 60.seconds // todo: move to config

  def selectPendingIndex(notifyActor: ActorRef, indexKey: IndexWorkersKey, db: Db)
                        (implicit ec: ExecutionContext, actorSystem: ActorSystem) = {
    db.selectPendingIndex(indexKey.partition, indexKey.documentUri, indexKey.indexId, indexKey.defTransactionId) map {
      case Some(pendingIndex) ⇒
        db.selectIndexDef(indexKey.documentUri, indexKey.indexId) map {
          case Some(indexDef) if indexDef.defTransactionId == pendingIndex.defTransactionId ⇒
            notifyActor ! BeginIndexing(indexDef, pendingIndex.lastItemSegment)
          case _ ⇒
            actorSystem.scheduler.scheduleOnce(RETRY_PERIOD, notifyActor, WaitForIndexDef(pendingIndex))
        }

      case None ⇒
        log.info(s"Can't find pending index for $indexKey, stopping actor")
        notifyActor ! CompletePendingIndex

    } recover {
      case NonFatal(e) ⇒
        log.error(s"Can't fetch pending index for $indexKey", e)
        actorSystem.scheduler.scheduleOnce(RETRY_PERIOD, notifyActor, StartPendingIndexWorker)
    }
  }

  def deletePendingIndex(notifyActor: ActorRef, pendingIndex: PendingIndex, db: Db)
                        (implicit ec: ExecutionContext, actorSystem: ActorSystem) = {
    db.deletePendingIndex(pendingIndex.partition, pendingIndex.documentUri, pendingIndex.indexId, pendingIndex.defTransactionId) map { _ ⇒

      log.warn(s"Pending index deleted: $pendingIndex (no corresponding index definition was found)")
      notifyActor ! CompletePendingIndex

    } recover {
      case NonFatal(e) ⇒
        log.error(s"Can't delete pending index $pendingIndex", e)
        actorSystem.scheduler.scheduleOnce(RETRY_PERIOD, notifyActor, StartPendingIndexWorker)
    }
  }
}
