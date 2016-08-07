package eu.inn.hyperstorage.indexing

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import eu.inn.hyperbus.Hyperbus
import eu.inn.hyperstorage.db.Db
import eu.inn.metrics.MetricsTracker
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

case object StartIndexWorker
case object WaitForIndexMeta
case object BeginIndexing

class IndexWorker (indexKey: IndexWorkersKey, hyperbus: Hyperbus, db: Db, tracker: MetricsTracker, bucketSize: Int)
  extends Actor with ActorLogging {

  override def preStart(): Unit = {
    super.preStart()
    self ! StartIndexWorker
  }

  /*
    1. check if pending index is still pending, otherwise terminate
    2. check if there is index meta corresponds to pending index, if not;
      2.1. setup timer to check later
      2.2. if later it didn't appeared, remove pending index
    3. start selecting from content from pending.last-segment and
      3.1. for each bucket, notify background-worker about indexing
    4. handle notification from background-worker about indexing:
      4.1. bucket-complete: select next bucket, if there is more
      4.2. index-complete: terminate
  */

  override def receive = indexingComplete orElse {
    case StartIndexWorker ⇒
      import context.dispatcher
      IndexWorkerImpl.selectPendingIndex(self, indexKey, db)

    case WaitForIndexMeta ⇒
      context.become(waitingForIndexMeta)
      IndexWorkerImpl.selectPendingIndex(self, indexKey, db)
  }

  def indexingComplete: Receive = {
    case IndexingComplete(ik) if indexKey == ik ⇒
      context.parent ! IndexingComplete(ik)
      context.stop(self)
  }

  def waitingForIndexMeta: Receive = indexingComplete orElse {
    case StartIndexWorker | WaitForIndexMeta ⇒
      // todo: didn't got index meta, assume transaction was incomplete and delete pending info
      context.parent ! IndexingComplete(indexKey)
      context.stop(self)
  }
}

object IndexWorker {
  def props(indexKey: IndexWorkersKey, hyperbus: Hyperbus, db: Db, tracker: MetricsTracker, bucketSize: Int) = Props(
    classOf[IndexWorker],indexKey, hyperbus, db, tracker, bucketSize
  )
}

private [indexing] object IndexWorkerImpl {
  val log = LoggerFactory.getLogger(getClass)
  import scala.concurrent.duration._
  val RETRY_PERIOD = 60.seconds

  def selectPendingIndex(notifyActor: ActorRef, indexKey: IndexWorkersKey, db: Db)
                        (implicit ec: ExecutionContext, actorSystem: ActorSystem) = {
    db.selectPendingIndex(indexKey.partition, indexKey.documentUri, indexKey.indexId) map {
      case Some(pendingIndex) ⇒
        db.selectIndexMeta(indexKey.documentUri, indexKey.indexId) map {
          case Some(indexMeta) ⇒
            // if (indexMeta.) compare metaTransactionId
          case None ⇒
            actorSystem.scheduler.scheduleOnce(RETRY_PERIOD, notifyActor, WaitForIndexMeta)
        }

      case None ⇒
        log.info(s"Can't find pending index for $indexKey, stopping actor")
        notifyActor ! IndexingComplete(indexKey)

    } recover {
      case NonFatal(e) ⇒
        log.error(s"Can't fetch pending index for $indexKey", e)
        actorSystem.scheduler.scheduleOnce(RETRY_PERIOD, notifyActor, StartIndexWorker)
    }
  }
}
