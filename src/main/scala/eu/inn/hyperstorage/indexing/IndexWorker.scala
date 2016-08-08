package eu.inn.hyperstorage.indexing

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import eu.inn.hyperbus.Hyperbus
import eu.inn.hyperstorage.{IndexTask, IndexTaskResult}
import eu.inn.hyperstorage.db.{Db, IndexMeta, PendingIndex}
import eu.inn.metrics.MetricsTracker
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

case object StartIndexWorker
case object CompletePendingIndex
case class BeginIndexing(indexMeta: IndexMeta, lastItemSegment: Option[String])
case class WaitForIndexMeta(pendingIndex: PendingIndex)
case class IndexNextTimeout(processId: Long)

// todo: add indexing progress log
class IndexWorker (cluster: ActorRef, indexKey: IndexWorkersKey, hyperbus: Hyperbus, db: Db, tracker: MetricsTracker)
  extends Actor with ActorLogging {

  override def preStart(): Unit = {
    super.preStart()
    self ! StartIndexWorker
  }

  override def receive = starOrStop orElse {
    case WaitForIndexMeta ⇒
      import context._
      become(waitingForIndexMeta)
      IndexWorkerImpl.selectPendingIndex(context.self, indexKey, db)
  }

  def starOrStop: Receive = {
    case StartIndexWorker ⇒
      import context._
      IndexWorkerImpl.selectPendingIndex(context.self, indexKey, db)

    case CompletePendingIndex ⇒
      context.parent ! IndexingComplete(indexKey)
      context.stop(self)

    case BeginIndexing(indexMeta, lastItemSegment) ⇒
      indexNext(0, indexMeta, lastItemSegment)
  }

  def waitingForIndexMeta: Receive = starOrStop orElse {
    case WaitForIndexMeta(pendingIndex) ⇒
      import context._
      IndexWorkerImpl.deletePendingIndex(context.self, pendingIndex, db)
  }

  def indexing(processId: Long, indexMeta: IndexMeta, lastItemSegment: Option[String]): Receive = {
    case IndexNextTimeout(p) if p == processId ⇒
      indexNext(processId + 1, indexMeta, lastItemSegment)

    case IndexTaskResult(Some(newLastItemSegment), p) if p == processId ⇒
      indexNext(processId + 1, indexMeta, Some(newLastItemSegment))

    case IndexTaskResult(None, p) if p == processId ⇒
      context.parent ! IndexingComplete(indexKey)
      context.stop(self)
  }

  def indexNext(processId: Long, indexMeta: IndexMeta, lastItemSegment: Option[String]): Unit = {
    import context.dispatcher
    context.become(indexing(processId, indexMeta, lastItemSegment))
    cluster ! IndexTask(System.currentTimeMillis() + IndexWorkerImpl.RETRY_PERIOD.toMillis,
      indexMeta, lastItemSegment, processId)
    context.system.scheduler.scheduleOnce(IndexWorkerImpl.RETRY_PERIOD*2, self, IndexNextTimeout(processId))
  }
}

object IndexWorker {
  def props(cluster: ActorRef, indexKey: IndexWorkersKey, hyperbus: Hyperbus, db: Db, tracker: MetricsTracker) = Props(
    classOf[IndexWorker], cluster: ActorRef, indexKey, hyperbus, db, tracker
  )
}

private [indexing] object IndexWorkerImpl {
  val log = LoggerFactory.getLogger(getClass)
  import scala.concurrent.duration._
  val RETRY_PERIOD = 60.seconds // todo: move to config

  def selectPendingIndex(notifyActor: ActorRef, indexKey: IndexWorkersKey, db: Db)
                        (implicit ec: ExecutionContext, actorSystem: ActorSystem) = {
    db.selectPendingIndex(indexKey.partition, indexKey.documentUri, indexKey.indexId, indexKey.metaTransactionId) map {
      case Some(pendingIndex) ⇒
        db.selectIndexMeta(indexKey.documentUri, indexKey.indexId) map {
          case Some(indexMeta) if indexMeta.metaTransactionId == pendingIndex.metaTransactionId ⇒
            notifyActor ! BeginIndexing(indexMeta, pendingIndex.lastItemSegment)
          case _ ⇒
            actorSystem.scheduler.scheduleOnce(RETRY_PERIOD, notifyActor, WaitForIndexMeta(pendingIndex))
        }

      case None ⇒
        log.info(s"Can't find pending index for $indexKey, stopping actor")
        notifyActor ! CompletePendingIndex

    } recover {
      case NonFatal(e) ⇒
        log.error(s"Can't fetch pending index for $indexKey", e)
        actorSystem.scheduler.scheduleOnce(RETRY_PERIOD, notifyActor, StartIndexWorker)
    }
  }

  def deletePendingIndex(notifyActor: ActorRef, pendingIndex: PendingIndex, db: Db)
                        (implicit ec: ExecutionContext, actorSystem: ActorSystem) = {
    db.deletePendingIndex(pendingIndex.partition, pendingIndex.documentUri, pendingIndex.indexId, pendingIndex.metaTransactionId) map { _ ⇒

      log.warn(s"Pending index deleted: $pendingIndex (no corresponding index meta was found)")
      notifyActor ! CompletePendingIndex

    } recover {
      case NonFatal(e) ⇒
        log.error(s"Can't delete pending index $pendingIndex", e)
        actorSystem.scheduler.scheduleOnce(RETRY_PERIOD, notifyActor, StartIndexWorker)
    }
  }
}
