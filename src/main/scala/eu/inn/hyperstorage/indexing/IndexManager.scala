package eu.inn.hyperstorage.indexing

import akka.actor.{Actor, ActorLogging, ActorRef, Terminated}
import eu.inn.hyperbus.Hyperbus
import eu.inn.hyperstorage.TransactionLogic
import eu.inn.hyperstorage.db.Db
import eu.inn.hyperstorage.sharding.ShardMemberStatus.{Active, Deactivating}
import eu.inn.hyperstorage.sharding.{ShardedClusterData, UpdateShardStatus}
import eu.inn.hyperstorage.utils.BiMap
import eu.inn.metrics.MetricsTracker

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

case object ShutdownIndexManager
case class ProcessNextPartitions(processId: Long)
case class IndexWorkersKey(partition: Int, documentUri: String, indexId: String)
case class PartitionPendingIndexes(partition: Int, rev: Long, indexes: Seq[IndexWorkersKey])
case class PartitionPendingFailed(rev: Long)

class IndexManager(hyperbus: Hyperbus, db: Db, tracker: MetricsTracker, maxIndexWorkers: Int)
  extends Actor with ActorLogging {

  val indexWorkers = BiMap[IndexWorkersKey, ActorRef]()
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

  def running(stateData: ShardedClusterData): Receive = {
    case UpdateShardStatus(_, Active, newStateData) ⇒
      if (newStateData != stateData) {
        // restart with new partition list
        clusterActivated(newStateData, TransactionLogic.getPartitions(stateData))
      }

    case UpdateShardStatus(_, Deactivating, _) ⇒
      indexWorkers.values.foreach(context.stop)
      context.become(stopping)

    case Terminated(actorRef) ⇒
      indexWorkers.inverse.get(actorRef).foreach(indexWorkers -= _)
      processPendingIndexes()

    case ProcessNextPartitions(processId) if processId == currentProcessId ⇒
      currentProcessId = currentProcessId + 1
      processPendingIndexes()

    case PartitionPendingIndexes(partition, msgRev, indexes) if rev == msgRev ⇒
      if (indexes.isEmpty) {
        pendingPartitions.remove(partition)
      }
      else {
        val alreadyPending = pendingPartitions.getOrElseUpdate(partition, mutable.ListBuffer.empty)
        val alreadyPendingSet = alreadyPending.toSet
        var updated = false
        indexes.foreach { index ⇒
          if (!alreadyPendingSet.contains(index)) {
            updated = true
            alreadyPending += index
          }
        }
        if (updated) {
          processPendingIndexes()
        }
      }

    case PartitionPendingFailed(msgRev) if rev == msgRev ⇒
      processPendingIndexes()
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
      case (v @ IndexWorkersKey(partition, _, _), actorRef) if detachedPartitions.contains(partition) ⇒
        context.stop(actorRef)
        Some(v)
      case _ ⇒
        None
    }
    // remove fromIndexWorkers before actor is stopped
    toRemove.foreach(indexWorkers -= _)

    val attachedPartitions = newPartitionSet diff previousPartitionSet
    pendingPartitions ++= attachedPartitions.map(_ → mutable.ListBuffer.empty[IndexWorkersKey])

    context.become(running(stateData))
    processPendingIndexes()
  }

  def processPendingIndexes(): Unit = {
    val availableWorkers = maxIndexWorkers - indexWorkers.size
    if (availableWorkers > 0 && pendingPartitions.nonEmpty) {
      createWorkerActors(availableWorkers)
      fetchPendingIndexes()
    }
  }

  def createWorkerActors(availableWorkers: Int): Unit = {
    nextPendingPartitions.flatMap(_._2).take(availableWorkers).foreach { key ⇒
      // createWorkingActor here
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
      IndexManager.fetchPendingIndexesFromDb(self, nextPartitionToFetch, rev, maxIndexWorkers, db)
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

  /*
      1. iterate new attached partitions and start PendingIndexWorker on pending indexes
        1.1. set refresh timer if no empty slots
      2. stop pending worker for detached partition
      3. on information about create/delete do the same as in 1.
     */
}

object IndexManager {
  def fetchPendingIndexesFromDb(notifyActor: ActorRef, partition: Int, rev: Long, maxIndexWorkers: Int, db: Db)
                               (implicit ec: ExecutionContext): Unit = {
    db.selectPendingIndexes(partition, maxIndexWorkers) map { indexesIterator ⇒
      notifyActor ! PartitionPendingIndexes(partition, rev,
        indexesIterator.map(ii ⇒ IndexWorkersKey(ii.partition, ii.documentUri, ii.indexId)).toSeq
      )
    } recover {
      case NonFatal(e) ⇒
        notifyActor ! PartitionPendingFailed(rev)
    }
  }
}
