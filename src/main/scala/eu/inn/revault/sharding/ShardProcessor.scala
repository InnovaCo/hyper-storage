package eu.inn.revault.sharding

import akka.actor._
import akka.cluster.ClusterEvent.{MemberEvent, MemberExited, MemberRemoved, MemberUp}
import akka.cluster.Member.addressOrdering
import akka.cluster.{Cluster, ClusterEvent, Member}
import akka.routing.{ConsistentHash, MurmurHash}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.control.NonFatal

@SerialVersionUID(1L) trait ShardTask {
  def key: String
  def group: String
  def isExpired: Boolean
}

sealed trait ShardMemberStatus
object ShardMemberStatus {
  @SerialVersionUID(1L) case object Unknown extends ShardMemberStatus
  @SerialVersionUID(1L) case object Passive extends ShardMemberStatus
  @SerialVersionUID(1L) case object Activating extends ShardMemberStatus
  @SerialVersionUID(1L) case object Active extends ShardMemberStatus
  @SerialVersionUID(1L) case object Deactivating extends ShardMemberStatus
}

@SerialVersionUID(1L) case class Sync(applicantAddress: Address, status: ShardMemberStatus, clusterHash: Int)
@SerialVersionUID(1L) case class SyncReply(acceptorAddress: Address, status: ShardMemberStatus, acceptedStatus: ShardMemberStatus, clusterHash: Int)
@SerialVersionUID(1L) case class NoSuchGroupWorkerException(groupName: String) extends RuntimeException(s"No such worker group: $groupName")

case class ShardMember(actorRef: ActorSelection,
                       status: ShardMemberStatus,
                       confirmedStatus: ShardMemberStatus)

case class ShardedClusterData(members: Map[Address, ShardMember], selfAddress: Address, selfStatus: ShardMemberStatus) {
  lazy val clusterHash = MurmurHash.stringHash(allMemberStatuses.map(_._1).mkString("|"))
  def + (elem: (Address, ShardMember)) = ShardedClusterData(members + elem, selfAddress, selfStatus)
  def - (key: Address) = ShardedClusterData(members - key, selfAddress, selfStatus)

  def taskIsFor(task: ShardTask): Address = consistentHash.nodeFor(task.key)

  def taskWasFor(task: ShardTask): Address = consistentHashPrevious.nodeFor(task.key)

  private lazy val consistentHash = ConsistentHash(activeMembers, VirtualNodesSize)

  private lazy val consistentHashPrevious = ConsistentHash(previouslyActiveMembers, VirtualNodesSize)

  private lazy val allMemberStatuses: List[(Address, ShardMemberStatus)] = {
    members.map {
      case (address, rvm) ⇒ address → rvm.status
    }.toList :+ (selfAddress → selfStatus)
  }.sortBy(_._1)

  private def VirtualNodesSize = 128 // todo: find a better value, configurable? http://www.tom-e-white.com/2007/11/consistent-hashing.html

  private def activeMembers: Iterable[Address] = allMemberStatuses.flatMap {
      case (address, ShardMemberStatus.Active) ⇒ Some(address)
      case (address, ShardMemberStatus.Activating) ⇒ Some(address)
      case _ ⇒ None
    }

  private def previouslyActiveMembers: Iterable[Address] = allMemberStatuses.flatMap {
    case (address, ShardMemberStatus.Active) ⇒ Some(address)
    case (address, ShardMemberStatus.Activating) ⇒ Some(address)
    case (address, ShardMemberStatus.Deactivating) ⇒ Some(address)
    case _ ⇒ None
  }
}

case class SubscribeToShardStatus(subscriber: ActorRef)
case class UpdateShardStatus(self: ActorRef, stateName: ShardMemberStatus, stateData: ShardedClusterData)

private [sharding] case object ShardSyncTimer
case object ShutdownProcessor
case class ShardTaskComplete(task: ShardTask, result: Any)

class ShardProcessor(workersSettings: Map[String, (Props, Int)],
                     roleName: String,
                     syncTimeout: FiniteDuration = 1000.millisecond)
                     extends FSMEx[ShardMemberStatus, ShardedClusterData] with Stash {

  private val cluster = Cluster(context.system)
  if (!cluster.selfRoles.contains(roleName)) {
    log.error(s"Cluster doesn't contains '$roleName' role. Please configure.")
  }
  private val selfAddress = cluster.selfAddress
  val activeWorkers = workersSettings.map { case (groupName, (props, maxCount)) ⇒
    groupName → mutable.ArrayBuffer[(ShardTask, ActorRef, ActorRef)]()
  }
  private val shardStatusSubscribers = mutable.MutableList[ActorRef]()
  cluster.subscribe(self, initialStateMode = ClusterEvent.InitialStateAsEvents, classOf[MemberEvent])

  startWith(ShardMemberStatus.Activating, ShardedClusterData(Map.empty, selfAddress, ShardMemberStatus.Activating))

  when(ShardMemberStatus.Activating) {
    case Event(MemberUp(member), data) ⇒
      introduceSelfTo(member, data) andUpdate

    case Event(ShardSyncTimer, data) ⇒
      if (isActivationAllowed(data)) {
        goto(ShardMemberStatus.Active)
      }
      else {
        stay
      }

    case Event(task: ShardTask, data) ⇒
      holdTask(task, data)
      stay
  }

  when(ShardMemberStatus.Active) {
    case Event(ShardSyncTimer, data) ⇒
      confirmStatus(data, ShardMemberStatus.Active, isFirst = false)
      stay

    case Event(ShutdownProcessor, data) ⇒
      confirmStatus(data, ShardMemberStatus.Deactivating, isFirst = true)
      setSyncTimer()
      goto(ShardMemberStatus.Deactivating) using data.copy(selfStatus = ShardMemberStatus.Deactivating)

    case Event(task: ShardTask, data) ⇒
      processTask(task, data)
      stay
  }

  when(ShardMemberStatus.Deactivating) {
    case Event(ShardSyncTimer, data) ⇒
      if(confirmStatus(data, ShardMemberStatus.Deactivating, isFirst = false)) {
        confirmStatus(data, ShardMemberStatus.Passive, isFirst = false) // not reliable
        stop()
      }
      else {
        stay
      }
  }

  whenUnhandled {
    case Event(sync: Sync, data) ⇒
      incomingSync(sync, data) andUpdate

    case Event(syncReply: SyncReply, data) ⇒
      processReply(syncReply, data) andUpdate

    case Event(MemberUp(member), data) ⇒
      addNewMember(member, data) andUpdate

    case Event(MemberRemoved(member, previousState), data) ⇒
      removeMember(member, data) andUpdate

    case Event(MemberExited(member), data) ⇒
      removeMember(member, data) andUpdate

    case Event(ShardTaskComplete(task, result), data) ⇒
      workerIsReadyForNextTask(task, result)
      stay()

    case Event(task: ShardTask, data) ⇒
      forwardTask(task, data)
      stay()

    case Event(SubscribeToShardStatus(actorRef), data) ⇒
      shardStatusSubscribers += actorRef
      actorRef ! UpdateShardStatus(self, stateName, data)
      stay()

    case Event(e, s) =>
      log.warning("received unhandled request {} in state {}/{}", e, stateName, s)
      stay
  }

  onTransition {
    case a -> b ⇒
      if (a != b) {
        log.info(s"Changing state from $a to $b")
        safeUnstashAll()
      }
  }

  initialize()

  def setSyncTimer(): Unit = {
    setTimer("syncing", ShardSyncTimer, syncTimeout)
  }

  override def processEventEx(event: Event, source: AnyRef): Unit = {
    val oldState = stateName
    val oldData = stateData
    super.processEventEx(event, source)
    if (oldData != stateData || oldState != stateName) {
      shardStatusSubscribers.foreach(_ ! UpdateShardStatus(sender, stateName, stateData))
    }
  }

  def introduceSelfTo(member: Member, data: ShardedClusterData) : Option[ShardedClusterData] = {
    if (member.hasRole(roleName) && member.address != selfAddress) {
        val actor = context.actorSelection(RootActorPath(member.address) / "user" / roleName)
        val newData = data + member.address → ShardMember(
          actor, ShardMemberStatus.Unknown, ShardMemberStatus.Unknown
        )
        val sync = Sync(selfAddress, ShardMemberStatus.Activating, newData.clusterHash)
        actor ! sync
        setSyncTimer()
        log.info(s"New member of shard cluster $roleName: $member. $sync was sent to $actor")
        Some(newData)
      }
    else if (member.hasRole(roleName) && member.address == selfAddress) {
      log.info(s"Self is up: $member on role $roleName")
      setSyncTimer()
      None
    }
    else {
      log.debug(s"Non shard member in $roleName is ignored: $member")
      None
    }
  }

  def processReply(syncReply: SyncReply, data: ShardedClusterData) : Option[ShardedClusterData] = {
    if (log.isDebugEnabled) {
      log.debug(s"$syncReply received from $sender")
    }
    if (syncReply.clusterHash != data.clusterHash) {
      log.info(s"ClusterHash (${data.clusterHash}) is not matched for $syncReply. SyncReply is ignored")
      None
    } else {
      data.members.get(syncReply.acceptorAddress) map { member ⇒
        data + syncReply.acceptorAddress →
          member.copy(status = syncReply.status, confirmedStatus = syncReply.acceptedStatus)
      } orElse {
        log.warning(s"Got $syncReply from unknown member of $roleName. Current members: ${data.members}")
        None
      }
    }
  }

  def isActivationAllowed(data: ShardedClusterData): Boolean = {
    if (confirmStatus(data, ShardMemberStatus.Activating, isFirst = false)) {
      log.info(s"Synced with all members: ${data.members}. Activating")
      confirmStatus(data, ShardMemberStatus.Active, isFirst = true)
      true
    }
    else {
      false
    }
  }

  def confirmStatus(data: ShardedClusterData, status: ShardMemberStatus, isFirst: Boolean) : Boolean = {
    var syncedWithAllMembers = true
    data.members.foreach { case (address, member) ⇒
      if (member.confirmedStatus != status) {
        syncedWithAllMembers = false
        val sync = Sync(selfAddress, status, data.clusterHash)
        member.actorRef ! sync
        setSyncTimer()
        if (log.isDebugEnabled && !isFirst) {
          log.debug(s"Didn't received reply from: $member. $sync was sent to ${member.actorRef}")
        }
      }
    }
    syncedWithAllMembers
  }

  def incomingSync(sync: Sync, data: ShardedClusterData): Option[ShardedClusterData] = {
    if (log.isDebugEnabled) {
      log.debug(s"$sync received from $sender")
    }
    if (sync.clusterHash != data.clusterHash) {
      log.info(s"ClusterHash (${data.clusterHash}) is not matched for $sync")
      None
    } else {
      data.members.get(sync.applicantAddress) map { member ⇒
        val newData: ShardedClusterData = data + sync.applicantAddress → member.copy(status = sync.status)
        val allowSync = if (sync.status == ShardMemberStatus.Activating) {
          activeWorkers.values.flatten.forall { case (task, workerActor, _) ⇒
            if (newData.taskIsFor(task) == sync.applicantAddress) {
              log.info(s"Ignoring sync request $sync while processing task $task by worker $workerActor")
              false
            } else {
              true
            }
          }
        } else {
          true
        }

        if (allowSync) {
          val syncReply = SyncReply(selfAddress, stateName, sync.status, data.clusterHash)
          if (log.isDebugEnabled) {
            log.debug(s"Replying with $syncReply to $sender")
          }
          sender() ! syncReply
        }
        newData
      } orElse {
        log.error(s"Got $sync from unknown member. Current members: ${data.members}")
        sender() ! SyncReply(selfAddress, stateName, ShardMemberStatus.Unknown, data.clusterHash)
        None
      }
    }
  }

  def addNewMember(member: Member, data: ShardedClusterData): Option[ShardedClusterData] = {
    if (member.hasRole(roleName) && member.address != selfAddress) {
      val actor = context.actorSelection(RootActorPath(member.address) / "user" / roleName)
      val newData = data + member.address → ShardMember(
        actor, ShardMemberStatus.Unknown, ShardMemberStatus.Unknown
      )
      if (log.isDebugEnabled) {
        log.info(s"New member in $roleName $member. State is unknown yet")
      }
      Some(newData)
    }
    else {
      None
    }
  }

  def removeMember(member: Member, data: ShardedClusterData): Option[ShardedClusterData] = {
    if (member.hasRole(roleName)) {
      if (log.isDebugEnabled) {
        log.info(s"Member removed from $roleName: $member.")
      }
      Some(data - member.address)
    } else {
      None
    }
  }

  def processTask(task: ShardTask, data: ShardedClusterData): Unit = {
    if (log.isDebugEnabled) {
      log.debug(s"Got task to process: $task")
    }
    if (task.isExpired) {
      log.warning(s"Task is expired, dropping: $task")
    } else {
      if (data.taskIsFor(task) == data.selfAddress) {
        if (data.taskWasFor(task) != data.selfAddress) {
          if (log.isDebugEnabled) {
            log.debug(s"Stashing task received for deactivating node: ${data.taskWasFor(task)}: $task")
          }
          safeStash(task)
        } else {
          activeWorkers.get(task.group) match {
            case Some(activeGroupWorkers) ⇒
              activeGroupWorkers.find(_._1.key == task.key) map { case (_, activeWorker, _) ⇒
                if (log.isDebugEnabled) {
                  log.debug(s"Stashing task for the 'locked' URL: $task worker: $activeWorker")
                }
                safeStash(task)
                true
              } getOrElse {
                val maxCount = workersSettings(task.group)._2
                if (activeGroupWorkers.size >= maxCount) {
                  if (log.isDebugEnabled) {
                    log.debug(s"Worker limit for group '${task.group}' is reached ($maxCount), stashing task: $task")
                  }
                  safeStash(task)
                } else {
                  val workerProps = workersSettings(task.group)._1
                  try {
                    val worker = context.system.actorOf(workerProps)
                    if (log.isDebugEnabled) {
                      log.debug(s"Forwarding task from ${sender()} to worker $worker: $task")
                    }
                    worker ! task
                    activeGroupWorkers.append((task, worker, sender()))
                  } catch {
                    case NonFatal(e) ⇒
                      log.error(e, s"Can't create worker from props $workerProps")
                      sender() ! e
                  }
                }
              }
            case None ⇒
              log.error(s"No such worker group: ${task.group}. Task is dismissed: $task")
              sender() ! new NoSuchGroupWorkerException(task.group)
          }
        }
      }
      else {
        forwardTask(task, data)
      }
    }
  }

  def holdTask(task: ShardTask, data: ShardedClusterData): Unit = {
    if (log.isDebugEnabled) {
      log.debug(s"Got task to process while activating: $task")
    }
    if (task.isExpired) {
      log.warning(s"Task is expired, dropping: $task")
    } else {
      if (data.taskIsFor(task) == data.selfAddress) {
        if (log.isDebugEnabled) {
          log.debug(s"Stashing task while activating: $task")
        }
        safeStash(task)
      } else {
        forwardTask(task, data)
      }
    }
  }

  def safeStash(task: ShardTask) = try {
    stash()
  } catch {
    case NonFatal(e) ⇒
      log.error(e, s"Can't stash task: $task. It's lost now")
  }

  def safeUnstashAll() = try {
    unstashAll()
  } catch {
    case NonFatal(e) ⇒
      log.error(e, s"Can't unstash tasks. Some are lost now")
  }

  def forwardTask(task: ShardTask, data: ShardedClusterData): Unit = {
    val address = data.taskIsFor(task)
    data.members.get(address) map { rvm ⇒
      if (log.isDebugEnabled) {
        log.debug(s"Task is forwarded to $address: $task")
      }
      rvm.actorRef forward task
      true
    } getOrElse {
      log.error(s"Task actor is not found: $address, dropping: $task")
    }
  }

  def workerIsReadyForNextTask(task: ShardTask, result: Any): Unit = {
    activeWorkers.get(task.group) match {
      case Some(activeGroupWorkers) ⇒
        val idx = activeGroupWorkers.indexWhere(_._2 == sender())
        if (idx >= 0) {
          val (task,worker,client) = activeGroupWorkers(idx)
          result match {
            case None ⇒ // no result
            case other ⇒ client ! other
          }
          if (log.isDebugEnabled) {
            log.debug(s"Worker $worker is ready for next task. Completed task: $task")
          }
          activeGroupWorkers.remove(idx)
          worker ! PoisonPill
          safeUnstashAll()
        } else {
          log.error(s"workerIsReadyForNextTask: unknown worker actor: $sender")
        }
      case None ⇒
        log.error(s"No such worker group: ${task.group}. Task result from $sender is ignored: $result. Task: $task")
    }
  }

  protected [this] implicit class ImplicitExtender(data: Option[ShardedClusterData]) {
    def andUpdate: State = {
      if (data.isDefined) {
        safeUnstashAll()
        stay using data.get
      }
      else {
        stay
      }
    }
  }
}
