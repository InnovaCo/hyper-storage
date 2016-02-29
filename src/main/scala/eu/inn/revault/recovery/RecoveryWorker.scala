package eu.inn.revault.recovery

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
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
            1.2. track list of self channels
            1.3. run hot recovery worker on channel
            1.4. run stale recovery worker on channel
            1.5. warns if hot isn't covered within hot period, extend hot period [range]
        2. hot data recovery worker
            2.1. selects data from channel for last (30) minutes
            2.1. sends tasks for recovery, waits each to answer (limit?)
        3. stale data recovery worker
            3.1. selects data from channel starting from last check, until now - 30min (hot level)
            3.2. sends tasks for recovery, waits each to anwser (indefinitely)
            3.3. updates check dates (with replication lag date)
*/

case object StartCheck
case object ShutdownRecoveryWorker
case class CheckQuantum[T](dtQuantum: Long, channels: Seq[Int], state: T)

abstract class RecoveryWorker[T](
                          db: Db,
                          shardProcessor: ActorRef,
                          retryPeriod: FiniteDuration,
                          recoveryCompleterTimeout: Timeout
                        ) extends Actor with ActorLogging {
  import context._

  def receive = {
    case UpdateShardStatus(_, Active, stateData) ⇒
      runRecoveryCheck(stateData, getMyChannels(stateData))

    case ShutdownRecoveryWorker ⇒
      context.stop(self)
  }

  def running(stateData: ShardedClusterData, myChannels: Seq[Int]): Receive = {
    case UpdateShardStatus(_, Active, newStateData) ⇒
      if (newStateData != stateData) {
        // restart with new channel list
        runRecoveryCheck(newStateData, getMyChannels(newStateData))
      }

    case UpdateShardStatus(_, Deactivating, _) ⇒
      context.unbecome()

    case StartCheck ⇒
      runNewRecoveryCheck(myChannels)

    case CheckQuantum(dtQuantum, channelsForQuantum, state: T) ⇒
      checkQuantum(dtQuantum, channelsForQuantum) map { _ ⇒
        runNextRecoveryCheck(CheckQuantum(dtQuantum, channelsForQuantum, state))
      } recover {
        case NonFatal(e) ⇒
          log.error(e, s"Quantum check for $dtQuantum is failed. Will retry in $retryPeriod")
          system.scheduler.scheduleOnce(retryPeriod, self, CheckQuantum(dtQuantum, channelsForQuantum, state))
      }

    case ShutdownRecoveryWorker ⇒
      println("shutting down...")
      context.become(shuttingDown)
  }

  def shuttingDown: Receive = {
    case StartCheck | CheckQuantum ⇒
      context.stop(self)
  }

  def runRecoveryCheck(stateData: ShardedClusterData, myChannels: Seq[Int]): Unit = {
    context.become(running(stateData, myChannels))
    self ! StartCheck
  }

  def runNewRecoveryCheck(myChannels: Seq[Int]): Unit
  def runNextRecoveryCheck(previous: CheckQuantum[T]): Unit

  def checkQuantum(dtQuantum: Long, myChannels: Seq[Int]): Future[Unit] = {
    log.debug(s"Running channel check for $dtQuantum")
    FutureUtils.serial(myChannels) { channel ⇒
      // todo: selectChannelMonitors selects body which isn't eficient
      db.selectChannelMonitors(dtQuantum, channel).flatMap { monitors ⇒
        val incompleteMonitors = monitors.toList.filter(_.completedAt.isEmpty).groupBy(_.uri).keys.toSeq
        FutureUtils.serial(incompleteMonitors) { case monitorUri ⇒

          val path = ContentLogic.splitPath(monitorUri)
          val task = RevaultCompleterTask(path._1,
            System.currentTimeMillis() + recoveryCompleterTimeout.duration.toMillis + 1000,
            monitorUri
          )
          shardProcessor.ask(task)(recoveryCompleterTimeout)
        } map { completerResults ⇒
          completerResults.foreach {
            case RevaultCompleterTaskResult(path, completedMonitors) ⇒
              // todo: delete abandon monitors
              log.debug(s"Recovery of '$path' completed successfully: $completedMonitors")
            case NoSuchResourceException(path) ⇒
              log.error(s"Tried to recover not existing resource: '$path'. Exception is ignored")
            case NonFatal(e) ⇒
              throw e
            case other ⇒
              throw new RuntimeException(s"Unexpected result from recovery task: $other")
          }
        }
      }
    } map(_ ⇒ {})
  }

  def getMyChannels(data: ShardedClusterData): Seq[Int] = {
    0 until MonitorLogic.MaxChannels flatMap { channel ⇒
      val task = new ShardTask { def key = channel.toString; def group = ""; def isExpired = false }
      if (data.taskIsFor(task) == data.selfAddress)
        Some(channel)
      else
        None
    }
  }
}

case class HotWorkerState(allChannels: Seq[Int])

class HotRecoveryWorker(
                         hotPeriod: (Long,Long),
                         db: Db,
                         shardProcessor: ActorRef,
                         retryPeriod: FiniteDuration,
                         recoveryCompleterTimeout: Timeout
                       ) extends RecoveryWorker[HotWorkerState] (
    db,shardProcessor,retryPeriod, recoveryCompleterTimeout
  ) {

  import context._

  def runNewRecoveryCheck(myChannels: Seq[Int]): Unit = {
    val millis = System.currentTimeMillis()
    val lowerBound = MonitorLogic.getDtQuantum(millis - hotPeriod._1)
    log.info(s"Running hot recovery check starting from $lowerBound")
    self ! CheckQuantum(lowerBound, myChannels, HotWorkerState(myChannels))
  }

  // todo: detect if lag is increasing and print warning
  def runNextRecoveryCheck(previous: CheckQuantum[HotWorkerState]): Unit = {
    val millis = System.currentTimeMillis()
    val upperBound = MonitorLogic.getDtQuantum(millis - hotPeriod._2)
    val nextQuantum = previous.dtQuantum + 1
    if (nextQuantum < upperBound) {
      self ! CheckQuantum(nextQuantum, previous.state.allChannels, previous.state)
    } else {
      log.info(s"Hot recovery complete on ${previous.dtQuantum}. Will start new in $retryPeriod")
      system.scheduler.scheduleOnce(retryPeriod, self, StartCheck)
    }
  }
}

case class StaleWorkerState(allChannels: Seq[Int], channelsPerQuantum: Map[Long, Seq[Int]])

//  We need StaleRecoveryWorker because of cassandra data can reappear later due to replication
class StaleRecoveryWorker(
                         stalePeriod: (Long,Long),
                         db: Db,
                         shardProcessor: ActorRef,
                         retryPeriod: FiniteDuration,
                         recoveryCompleterTimeout: Timeout) extends RecoveryWorker[StaleWorkerState] (
  db,shardProcessor,retryPeriod, recoveryCompleterTimeout
  ) {

  import context._

  def runNewRecoveryCheck(myChannels: Seq[Int]): Unit = {
    val lowerBound = MonitorLogic.getDtQuantum(System.currentTimeMillis() - stalePeriod._1)

    FutureUtils.serial(myChannels) { channel ⇒
      db.selectCheckpoint(channel) map {
        case Some(lastQuantum) ⇒
          lastQuantum → channel
        case None ⇒
          lowerBound → channel
      }
    } map { channelQuantums ⇒

      val stalest = channelQuantums.sortBy(_._1).head._1
      val channelsPerQuantum: Map[Long, Seq[Int]] = channelQuantums.groupBy(_._1).map(kv ⇒ kv._1 → kv._2.map(_._2))
      val state = StaleWorkerState(myChannels, channelsPerQuantum)
      val (startFrom,channels) = if (stalest < lowerBound) {
        (stalest, channelsPerQuantum(stalest))
      } else {
        (lowerBound, myChannels)
      }

      log.info(s"Running stale recovery check starting from $startFrom")
      self ! CheckQuantum(startFrom, channels, state)
    } recover {
      case NonFatal(e) ⇒
        log.error(e, s"Can't fetch checkpoints. Will retry in $retryPeriod")
        system.scheduler.scheduleOnce(retryPeriod, self, StartCheck)
    }
  }

  // todo: detect if lag is increasing and print warning
  def runNextRecoveryCheck(previous: CheckQuantum[StaleWorkerState]): Unit = {
    val millis = System.currentTimeMillis()
    val lowerBound = MonitorLogic.getDtQuantum(millis - stalePeriod._1)
    val upperBound = MonitorLogic.getDtQuantum(millis - stalePeriod._2)
    val nextQuantum = previous.dtQuantum + 1

    println(s"millis = $millis, lowerBound = $lowerBound, upperBound = $upperBound, nextQuantum = $nextQuantum")

    val futureUpdateCheckpoints = if (previous.dtQuantum <= lowerBound) {
      FutureUtils.serial(previous.channels) { channel ⇒
        db.updateCheckpoint(channel, previous.dtQuantum)
      }
    } else {
      Future.successful({})
    }

    futureUpdateCheckpoints map { _ ⇒
      if (nextQuantum < upperBound) {
        if (nextQuantum >= lowerBound || previous.channels == previous.state.allChannels) {
          println("a")
          self ! CheckQuantum(nextQuantum, previous.state.allChannels, previous.state)
        } else {
          println("b")
          val nextQuantumChannels = previous.state.channelsPerQuantum.getOrElse(nextQuantum, Seq.empty)
          val channels = (previous.channels.toSet ++ nextQuantumChannels).toSeq
          self ! CheckQuantum(nextQuantum, channels, previous.state)
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

