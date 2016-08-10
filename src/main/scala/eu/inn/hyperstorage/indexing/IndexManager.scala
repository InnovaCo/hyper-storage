package eu.inn.hyperstorage.indexing

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import eu.inn.hyperbus.Hyperbus
import eu.inn.hyperstorage.TransactionLogic
import eu.inn.hyperstorage.db.Db
import eu.inn.hyperstorage.sharding.ShardMemberStatus.{Active, Deactivating}
import eu.inn.hyperstorage.sharding.{ShardedClusterData, UpdateShardStatus}
import eu.inn.metrics.MetricsTracker

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

case object ShutdownIndexManager
case class ProcessNextPartitions(processId: Long)

// todo: rename
// todo: partition can be always generated!
case class IndexWorkersKey(partition: Int, documentUri: String, indexId: String, metaTransactionId: UUID)

// todo: rename those two
case class PartitionPendingIndexes(partition: Int, rev: Long, indexes: Seq[IndexWorkersKey])
case class PartitionPendingFailed(rev: Long)
case class IndexCreatedOrDeleted(key: IndexWorkersKey)
case class IndexingComplete(key: IndexWorkersKey)

// todo: handle child termination without IndexingComplete

class IndexManager(hyperbus: Hyperbus, db: Db, tracker: MetricsTracker, maxIndexWorkers: Int)
  extends Actor with ActorLogging {

  val indexWorkers = mutable.Map[IndexWorkersKey, ActorRef]()
  val pendingPartitions = mutable.Map[Int, mutable.ListBuffer[IndexWorkersKey]]()
  var rev: Long = 0
  var currentProcessId: Long = 0

  override def receive: Receive = {
    case UpdateShardStatus(_, Active, stateData) ⇒
      clusterActivated(stateData, Seq.empty)

    case ShutdownIndexManager ⇒
      context.stop(self)
  }

  def stopping: Receive = {
    case ShutdownIndexManager ⇒
      context.stop(self)
  }

  def running(clusterActor: ActorRef, stateData: ShardedClusterData): Receive = {
    case UpdateShardStatus(_, Active, newStateData) ⇒
      if (newStateData != stateData) {
        // restart with new partition list
        clusterActivated(newStateData, TransactionLogic.getPartitions(stateData))
      }

    case UpdateShardStatus(_, Deactivating, _) ⇒
      indexWorkers.values.foreach(context.stop)
      context.become(stopping)

    case IndexingComplete(key) ⇒
      indexWorkers -= key
      processPendingIndexes(clusterActor)

    case ProcessNextPartitions(processId) if processId == currentProcessId ⇒
      currentProcessId = currentProcessId + 1
      processPendingIndexes(clusterActor)

    case PartitionPendingIndexes(partition, msgRev, indexes) if rev == msgRev ⇒
      if (indexes.isEmpty) {
        pendingPartitions.remove(partition)
      }
      else {
        val updated = indexes.foldLeft(false) { (updated,index) ⇒
          addPendingIndex(index) || updated
        }
        if (updated) {
          processPendingIndexes(clusterActor)
        }
      }

    case PartitionPendingFailed(msgRev) if rev == msgRev ⇒
      processPendingIndexes(clusterActor)

    case IndexCreatedOrDeleted(key) ⇒
      val partitionSet = TransactionLogic.getPartitions(stateData).toSet
      if (partitionSet.contains(key.partition)) {
        if (addPendingIndex(key)) {
          processPendingIndexes(clusterActor)
        }
      }
      else {
        log.info(s"Received $key update but partition is handled by other node, ignored")
      }
  }

  def addPendingIndex(key: IndexWorkersKey): Boolean = {
    val alreadyPending = pendingPartitions.getOrElseUpdate(key.partition, mutable.ListBuffer.empty)
    if (!alreadyPending.contains(key) && !indexWorkers.contains(key) && alreadyPending.size < maxIndexWorkers) {
      alreadyPending += key
      true
    }
    else {
      false
    }
  }

  def clusterActivated(stateData: ShardedClusterData,
                       previousPartitions: Seq[Int]): Unit = {
    rev = rev + 1
    log.info(s"Cluster is active $getClass is running. Current data: $stateData. rev=$rev")

    val newPartitions = TransactionLogic.getPartitions(stateData)
    val newPartitionSet = newPartitions.toSet
    val previousPartitionSet = previousPartitions.toSet
    val detachedPartitions = previousPartitionSet diff newPartitionSet

    // stop workers for detached partitions
    val toRemove = indexWorkers.flatMap {
      case (v @ IndexWorkersKey(partition, _, _, _), actorRef) if detachedPartitions.contains(partition) ⇒
        context.stop(actorRef)
        Some(v)
      case _ ⇒
        None
    }
    // remove fromIndexWorkers before actor is stopped
    toRemove.foreach(indexWorkers -= _)

    val attachedPartitions = newPartitionSet diff previousPartitionSet
    pendingPartitions ++= attachedPartitions.map(_ → mutable.ListBuffer.empty[IndexWorkersKey])

    context.become(running(sender(), stateData))
    processPendingIndexes(sender())
  }

  def processPendingIndexes(clusterActor: ActorRef): Unit = {
    val availableWorkers = maxIndexWorkers - indexWorkers.size
    if (availableWorkers > 0 && pendingPartitions.nonEmpty) {
      createWorkerActors(clusterActor, availableWorkers)
      fetchPendingIndexes()
    }
  }

  def createWorkerActors(clusterActor: ActorRef, availableWorkers: Int): Unit = {
    nextPendingPartitions.flatMap(_._2).take(availableWorkers).foreach { key ⇒
      // createWorkingActor here
      val actorRef = context.actorOf(IndexWorker.props(
        clusterActor, key, hyperbus, db, tracker
      ))
      indexWorkers += key → actorRef
      pendingPartitions(key.partition) -= key
    }
  }

  def fetchPendingIndexes(): Unit = {
    nextPendingPartitions.flatMap {
      case(k,v) if v.isEmpty ⇒  Some(k)
      case _ ⇒ None
    }.headOption.foreach { nextPartitionToFetch ⇒
      // async fetch and send as a message next portion of indexes along with `rev`
      import context.dispatcher
      IndexManagerImpl.fetchPendingIndexesFromDb(self, nextPartitionToFetch, rev, maxIndexWorkers, db)
    }
  }

  def nextPendingPartitions: Vector[(Int, Seq[IndexWorkersKey])] = {
    // move consequently (but not strictly) over pending partitions
    // because currentProcessId is always incremented
    val v = pendingPartitions.toVector
    val startFrom = currentProcessId % v.size
    val vn = if (startFrom == 0) { v } else {
      val vnp = v.splitAt(startFrom.toInt)
      vnp._2 ++ vnp._1
    }
    vn
  }
}

object IndexManager {
  def props(hyperbus: Hyperbus, db: Db, tracker: MetricsTracker, maxIndexWorkers: Int) = Props(classOf[IndexManager],
    hyperbus, db, tracker, maxIndexWorkers
  )
}

private [indexing] object IndexManagerImpl {
  def fetchPendingIndexesFromDb(notifyActor: ActorRef, partition: Int, rev: Long, maxIndexWorkers: Int, db: Db)
                               (implicit ec: ExecutionContext): Unit = {
    db.selectPendingIndexes(partition, maxIndexWorkers) map { indexesIterator ⇒
      notifyActor ! PartitionPendingIndexes(partition, rev,
        indexesIterator.map(ii ⇒ IndexWorkersKey(ii.partition, ii.documentUri, ii.indexId, ii.metaTransactionId)).toSeq
      )
    } recover {
      case NonFatal(e) ⇒
        notifyActor ! PartitionPendingFailed(rev)
    }
  }
}

