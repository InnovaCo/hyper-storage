package eu.inn.revault.recovery

import akka.actor._
import akka.pattern.ask
import eu.inn.revault._
import eu.inn.revault.db.Db
import eu.inn.revault.sharding.ShardMemberStatus.{Active, Deactivating}
import eu.inn.revault.sharding.{ShardTask, ShardedClusterData, UpdateShardStatus}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

/*
 * recovery job:
        1. recovery control actor
            1.1. track cluster data
            1.2. track list of self partitions
            1.3. run hot recovery worker on partition
            1.4. run stale recovery worker on partition
            1.5. warns if hot isn't covered within hot period, extend hot period [range]
        2. hot data recovery worker
            2.1. selects data from partition for last (30) minutes
            2.1. sends tasks for recovery, waits each to answer (limit?)
        3. stale data recovery worker
            3.1. selects data from partition starting from last check, until now - 30min (hot level)
            3.2. sends tasks for recovery, waits each to anwser (indefinitely)
            3.3. updates check dates (with replication lag date)
*/

case object StartCheck
case object ShutdownRecoveryWorker
case class CheckQuantum[T](dtQuantum: Long, partitions: Seq[Int], state: T)

abstract class RecoveryWorker[T](
                                  db: Db,
                                  shardProcessor: ActorRef,
                                  retryPeriod: FiniteDuration,
                                  completerTimeout: FiniteDuration
                                ) extends Actor with ActorLogging {
  import context._

  def receive = {
    case UpdateShardStatus(_, Active, stateData) ⇒
      runRecoveryCheck(stateData, getMyPartitions(stateData))

    case ShutdownRecoveryWorker ⇒
      context.stop(self)
  }

  def running(stateData: ShardedClusterData, workerPartitions: Seq[Int]): Receive = {
    case UpdateShardStatus(_, Active, newStateData) ⇒
      if (newStateData != stateData) {
        // restart with new partition list
        runRecoveryCheck(newStateData, getMyPartitions(newStateData))
      }

    case UpdateShardStatus(_, Deactivating, _) ⇒
      context.unbecome()

    case StartCheck ⇒
      runNewRecoveryCheck(workerPartitions)

    case CheckQuantum(dtQuantum, partitionsForQuantum, state) ⇒
      checkQuantum(dtQuantum, partitionsForQuantum) map { _ ⇒
        runNextRecoveryCheck(CheckQuantum(dtQuantum, partitionsForQuantum, state.asInstanceOf[T]))
      } recover {
        case NonFatal(e) ⇒
          log.error(e, s"Quantum check for $dtQuantum is failed. Will retry in $retryPeriod")
          system.scheduler.scheduleOnce(retryPeriod, self, CheckQuantum(dtQuantum, partitionsForQuantum, state))
      }

    case ShutdownRecoveryWorker ⇒
      log.info(s"$self is shutting down...")
      context.become(shuttingDown)
  }

  def shuttingDown: Receive = {
    case StartCheck | _ : CheckQuantum[_] ⇒
      context.stop(self)
  }

  def runRecoveryCheck(stateData: ShardedClusterData, partitions: Seq[Int]): Unit = {
    log.info(s"Cluster is active, starting recovery check. Current data: $stateData. Partitions to process: ${partitions.size}")
    context.become(running(stateData, partitions))
    self ! StartCheck
  }

  def runNewRecoveryCheck(partitions: Seq[Int]): Unit
  def runNextRecoveryCheck(previous: CheckQuantum[T]): Unit

  def checkQuantum(dtQuantum: Long, partitions: Seq[Int]): Future[Unit] = {
    log.debug(s"Running partition check for $dtQuantum")
    FutureUtils.serial(partitions) { partition ⇒
      // todo: selectPartitionTransactions selects body which isn't eficient
      db.selectPartitionTransactions(dtQuantum, partition).flatMap { partitionTransactions ⇒
        val incompleteTransactions = partitionTransactions.toList.filter(_.completedAt.isEmpty).groupBy(_.uri)
        FutureUtils.serial(incompleteTransactions.toSeq) { case (transactionUri, transactions) ⇒

          val path = ContentLogic.splitPath(transactionUri)
          val task = RevaultCompleterTask(path._1,
            System.currentTimeMillis() + completerTimeout.toMillis + 1000,
            transactionUri
          )
          log.debug(s"Incomplete resource at $transactionUri. Sending recovery task")
          shardProcessor.ask(task)(completerTimeout) flatMap {
            case RevaultCompleterTaskResult(completePath, completedTransactions) ⇒
              log.debug(s"Recovery of '$completePath' completed successfully: $completedTransactions")
              if (path._1 == completePath) {
                val set = completedTransactions.toSet
                val abandonedTransactions = transactions.filterNot(m ⇒ set.contains(m.uuid))
                if (abandonedTransactions.nonEmpty) {
                  log.warning(s"Abandoned transactions for '$completePath' were found: '${abandonedTransactions.map(_.uuid).mkString(",")}'. Deleting...")
                  FutureUtils.serial(abandonedTransactions.toSeq) { abandonedTransaction ⇒
                    db.completeTransaction(abandonedTransaction)
                  }
                } else {
                  Future.successful()
                }
              }
              else {
                log.error(s"Recovery result received for '$completePath' while expecting for the '$path'")
                Future.successful()
              }
            case NoSuchResourceException(notFountPath) ⇒
              log.error(s"Tried to recover not existing resource: '$notFountPath'. Exception is ignored")
              Future.successful()
            case (NonFatal(e), _) ⇒
              Future.failed(e)
            case other ⇒
              Future.failed(throw new RuntimeException(s"Unexpected result from recovery task: $other"))
          }
        }
      }
    } map(_ ⇒ {})
  }

  def getMyPartitions(data: ShardedClusterData): Seq[Int] = {
    0 until TransactionLogic.MaxPartitions flatMap { partition ⇒
      val task = new ShardTask { def key = partition.toString; def group = ""; def isExpired = false }
      if (data.taskIsFor(task) == data.selfAddress)
        Some(partition)
      else
        None
    }
  }
}

case class HotWorkerState(workerPartitions: Seq[Int])

class HotRecoveryWorker(
                         hotPeriod: (Long,Long),
                         db: Db,
                         shardProcessor: ActorRef,
                         retryPeriod: FiniteDuration,
                         recoveryCompleterTimeout: FiniteDuration
                       ) extends RecoveryWorker[HotWorkerState] (
    db,shardProcessor,retryPeriod, recoveryCompleterTimeout
  ) {

  import context._

  def runNewRecoveryCheck(partitions: Seq[Int]): Unit = {
    val millis = System.currentTimeMillis()
    val lowerBound = TransactionLogic.getDtQuantum(millis - hotPeriod._1)
    log.info(s"Running hot recovery check starting from $lowerBound")
    self ! CheckQuantum(lowerBound, partitions, HotWorkerState(partitions))
  }

  // todo: detect if lag is increasing and print warning
  def runNextRecoveryCheck(previous: CheckQuantum[HotWorkerState]): Unit = {
    val millis = System.currentTimeMillis()
    val upperBound = TransactionLogic.getDtQuantum(millis - hotPeriod._2)
    val nextQuantum = previous.dtQuantum + 1
    if (nextQuantum < upperBound) {
      self ! CheckQuantum(nextQuantum, previous.state.workerPartitions, previous.state)
    } else {
      log.info(s"Hot recovery complete on ${previous.dtQuantum}. Will start new in $retryPeriod")
      system.scheduler.scheduleOnce(retryPeriod, self, StartCheck)
    }
  }
}

case class StaleWorkerState(workerPartitions: Seq[Int], partitionsPerQuantum: Map[Long, Seq[Int]])

//  We need StaleRecoveryWorker because of cassandra data can reappear later due to replication
class StaleRecoveryWorker(
                           stalePeriod: (Long,Long),
                           db: Db,
                           shardProcessor: ActorRef,
                           retryPeriod: FiniteDuration,
                           completerTimeout: FiniteDuration
                         ) extends RecoveryWorker[StaleWorkerState] (
  db,shardProcessor,retryPeriod, completerTimeout
  ) {

  import context._

  def runNewRecoveryCheck(partitions: Seq[Int]): Unit = {
    val lowerBound = TransactionLogic.getDtQuantum(System.currentTimeMillis() - stalePeriod._1)

    FutureUtils.serial(partitions) { partition ⇒
      db.selectCheckpoint(partition) map {
        case Some(lastQuantum) ⇒
          lastQuantum → partition
        case None ⇒
          lowerBound → partition
      }
    } map { partitionQuantums ⇒

      val stalest = partitionQuantums.sortBy(_._1).head._1
      val partitionsPerQuantum: Map[Long, Seq[Int]] = partitionQuantums.groupBy(_._1).map(kv ⇒ kv._1 → kv._2.map(_._2))
      val state = StaleWorkerState(partitions, partitionsPerQuantum)
      val (startFrom,partitionsToProcess) = if (stalest < lowerBound) {
        (stalest, partitionsPerQuantum(stalest))
      } else {
        (lowerBound, partitions)
      }

      log.info(s"Running stale recovery check starting from $startFrom")
      self ! CheckQuantum(startFrom, partitionsToProcess, state)
    } recover {
      case NonFatal(e) ⇒
        log.error(e, s"Can't fetch checkpoints. Will retry in $retryPeriod")
        system.scheduler.scheduleOnce(retryPeriod, self, StartCheck)
    }
  }

  // todo: detect if lag is increasing and print warning
  def runNextRecoveryCheck(previous: CheckQuantum[StaleWorkerState]): Unit = {
    val millis = System.currentTimeMillis()
    val lowerBound = TransactionLogic.getDtQuantum(millis - stalePeriod._1)
    val upperBound = TransactionLogic.getDtQuantum(millis - stalePeriod._2)
    val nextQuantum = previous.dtQuantum + 1

    val futureUpdateCheckpoints = if (previous.dtQuantum <= lowerBound) {
      FutureUtils.serial(previous.partitions) { partition ⇒
        db.updateCheckpoint(partition, previous.dtQuantum)
      }
    } else {
      Future.successful({})
    }

    futureUpdateCheckpoints map { _ ⇒
      if (nextQuantum < upperBound) {
        if (nextQuantum >= lowerBound || previous.partitions == previous.state.workerPartitions) {
          self ! CheckQuantum(nextQuantum, previous.state.workerPartitions, previous.state)
        } else {
          val nextQuantumPartitions = previous.state.partitionsPerQuantum.getOrElse(nextQuantum, Seq.empty)
          val partitions = (previous.partitions.toSet ++ nextQuantumPartitions).toSeq
          self ! CheckQuantum(nextQuantum, partitions, previous.state)
        }
      }
      else {
        log.info(s"Stale recovery complete on ${previous.dtQuantum}. Will start new in $retryPeriod")
        system.scheduler.scheduleOnce(retryPeriod, self, StartCheck)
      }
    } recover {
      case NonFatal(e) ⇒
        log.error(e, s"Can't update checkpoints. Will restart in $retryPeriod")
        system.scheduler.scheduleOnce(retryPeriod, self, StartCheck)
    }
  }
}

