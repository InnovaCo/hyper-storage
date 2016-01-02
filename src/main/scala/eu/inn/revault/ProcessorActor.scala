package eu.inn.revault

import akka.actor._
import akka.cluster.ClusterEvent.{MemberEvent, MemberRemoved, MemberUp}
import akka.cluster.{Cluster, ClusterEvent, Member}
import akka.routing.{MurmurHash, ConsistentHash}
import scala.collection.mutable
import scala.concurrent.duration._
import akka.cluster.Member.addressOrdering

trait Expireable {
  def isExpired: Boolean
}

case class Task(key: String, content: Expireable)

sealed trait RevaultMemberStatus
object RevaultMemberStatus {
  @SerialVersionUID(1L) case object Unknown extends RevaultMemberStatus
  @SerialVersionUID(1L) case object Passive extends RevaultMemberStatus
  @SerialVersionUID(1L) case object Activating extends RevaultMemberStatus
  @SerialVersionUID(1L) case object Active extends RevaultMemberStatus
  @SerialVersionUID(1L) case object Deactivating extends RevaultMemberStatus
}

@SerialVersionUID(1L) case class Sync(applicantAddress: Address, status: RevaultMemberStatus, clusterHash: Int)
@SerialVersionUID(1L) case class SyncReply(confirmerAddress: Address, status: RevaultMemberStatus, confirmedStatus: RevaultMemberStatus, clusterHash: Int)

case class RevaultMemberActor(actorRef: ActorSelection,
                              status: RevaultMemberStatus,
                              confirmedStatus: RevaultMemberStatus)

// todo: rename class
case class ProcessorData(members: Map[Address, RevaultMemberActor], selfAddress: Address, selfStatus: RevaultMemberStatus) {
  lazy val clusterHash = MurmurHash.stringHash(allMemberStatuses.map(_._1).mkString("|"))
  def + (elem: (Address, RevaultMemberActor)) = ProcessorData(members + elem, selfAddress, selfStatus)
  def - (key: Address) = ProcessorData(members - key, selfAddress, selfStatus)

  def taskIsFor(task: Task): Address = consistentHash.nodeFor(task.key)

  def taskWasFor(task: Task): Address = consistentHashPrevious.nodeFor(task.key)

  private lazy val consistentHash = ConsistentHash(activeMembers, VirtualNodesSize)

  private lazy val consistentHashPrevious = ConsistentHash(previouslyActiveMembers, VirtualNodesSize)

  private lazy val allMemberStatuses: List[(Address, RevaultMemberStatus)] = {
    members.map {
      case (address, rvm) ⇒ address → rvm.status
    }.toList :+ (selfAddress → selfStatus)
  }.sortBy(_._1)

  private def VirtualNodesSize = 128 // todo: find a better value, configurable? http://www.tom-e-white.com/2007/11/consistent-hashing.html

  private def activeMembers: Iterable[Address] = allMemberStatuses.flatMap {
      case (address, RevaultMemberStatus.Active) ⇒ Some(address)
      case (address, RevaultMemberStatus.Activating) ⇒ Some(address)
      case _ ⇒ None
    }

  private def previouslyActiveMembers: Iterable[Address] = allMemberStatuses.flatMap {
    case (address, RevaultMemberStatus.Active) ⇒ Some(address)
    case (address, RevaultMemberStatus.Activating) ⇒ Some(address)
    case (address, RevaultMemberStatus.Deactivating) ⇒ Some(address)
    case _ ⇒ None
  }
}

sealed trait Events
case object SyncTimer extends Events
case object ShutdownProcessor extends Events
case object ReadyForNextTask

// todo: change name
class ProcessorFSM(workerProps: Props, workerCount: Int) extends FSM[RevaultMemberStatus, ProcessorData] with Stash {
  private val cluster = Cluster(context.system)
  private val selfAddress = cluster.selfAddress
  val inactiveWorkers: mutable.Stack[ActorRef] = mutable.Stack.tabulate(workerCount) { workerIndex ⇒
    context.system.actorOf(workerProps, s"revault-wrkr-$workerIndex")
  }
  val activeWorkers = mutable.ArrayBuffer[(Task, ActorRef)]()
  cluster.subscribe(self, initialStateMode = ClusterEvent.InitialStateAsEvents, classOf[MemberEvent])

  startWith(RevaultMemberStatus.Activating, ProcessorData(Map.empty, selfAddress, RevaultMemberStatus.Activating))

  when(RevaultMemberStatus.Activating) {
    case Event(MemberUp(member), data) ⇒
      introduceSelfTo(member, data) andUpdate

    case Event(SyncTimer, data) ⇒
      if (isActivationAllowed(data)) {
        goto(RevaultMemberStatus.Active)
      }
      else {
        stay
      }

    case Event(task: Task, data) ⇒
      holdTask(task, data)
      stay
  }

  when(RevaultMemberStatus.Active) {
    case Event(SyncTimer, data) ⇒
      confirmStatus(data, RevaultMemberStatus.Active, isFirst = false)
      stay

    case Event(ShutdownProcessor, data) ⇒
      confirmStatus(data, RevaultMemberStatus.Deactivating, isFirst = true)
      setSyncTimer()
      goto(RevaultMemberStatus.Deactivating)

    case Event(task: Task, data) ⇒
      processTask(task, data)
      stay
  }

  when(RevaultMemberStatus.Deactivating) {
    case Event(SyncTimer, data) ⇒
      if(confirmStatus(data, RevaultMemberStatus.Deactivating, isFirst = false)) {
        confirmStatus(data, RevaultMemberStatus.Passive, isFirst = false) // not reliable
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

    case Event(ReadyForNextTask, data) ⇒
      workerIsReadyForNextTask()
      stay()

    case Event(task: Task, data) ⇒
      forwardTask(task, data)
      stay()

    case Event(e, s) =>
      log.warning("received unhandled request {} in state {}/{}", e, stateName, s)
      stay
  }

  onTransition {
    case a -> b ⇒
      log.info(s"Changing state from $a to $b")
      unstashAll()
  }

  initialize()

  def setSyncTimer(): Unit = {
    setTimer("syncing", SyncTimer, 500 millisecond) // todo: move timeout to configuration
  }

  def introduceSelfTo(member: Member, data: ProcessorData) : Option[ProcessorData] = {
    if (member.hasRole("revault") && member.address != selfAddress) {
        val actor = context.actorSelection(RootActorPath(member.address) / "user" / "revault")
        val newData = data + member.address → RevaultMemberActor(
          actor, RevaultMemberStatus.Unknown, RevaultMemberStatus.Unknown
        )
        val sync = Sync(selfAddress, RevaultMemberStatus.Activating, newData.clusterHash)
        actor ! sync
        setSyncTimer()
        if (log.isDebugEnabled) {
          log.debug(s"New member: $member. $sync was sent to $actor")
        }
        Some(newData)
      }
    else if (member.hasRole("revault") && member.address == selfAddress) {
      log.debug(s"Self is up: $member")
      setSyncTimer()
      None
    }
    else {
      log.debug(s"Non revault member: $member is ignored")
      None
    }
  }

  def processReply(syncReply: SyncReply, data: ProcessorData) : Option[ProcessorData] = {
    if (log.isDebugEnabled) {
      log.debug(s"$syncReply received from $sender")
    }
    if (syncReply.clusterHash != data.clusterHash) {
      log.info(s"ClusterHash (${data.clusterHash}) is not matched for $syncReply. SyncReply is ignored")
      None
    } else {
      data.members.get(syncReply.confirmerAddress) map { member ⇒
        data + syncReply.confirmerAddress →
          member.copy(status = syncReply.status, confirmedStatus = syncReply.confirmedStatus)
      } orElse {
        log.warning(s"Got $syncReply from unknown member. Current members: ${data.members}")
        None
      }
    }
  }

  def isActivationAllowed(data: ProcessorData): Boolean = {
    if (confirmStatus(data, RevaultMemberStatus.Activating, isFirst = false)) {
      log.info(s"Synced with all members: ${data.members}. Activating")
      confirmStatus(data, RevaultMemberStatus.Active, isFirst = true)
      true
    }
    else {
      false
    }
  }

  def confirmStatus(data: ProcessorData, status: RevaultMemberStatus, isFirst: Boolean) : Boolean = {
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

  def incomingSync(sync: Sync, data: ProcessorData): Option[ProcessorData] = {
    if (log.isDebugEnabled) {
      log.debug(s"$sync received from $sender")
    }
    if (sync.clusterHash != data.clusterHash) {
      log.info(s"ClusterHash (${data.clusterHash}) is not matched for $sync")
      None
    } else {
      data.members.get(sync.applicantAddress) map { member ⇒
        // todo: don't confirm, until it's possible
        val syncReply = SyncReply(selfAddress, stateName, sync.status, data.clusterHash)
        if (log.isDebugEnabled) {
          log.debug(s"Replying with $syncReply to $sender")
        }
        sender() ! syncReply
        data + sync.applicantAddress → member.copy(status = sync.status)
      } orElse {
        log.error(s"Got $sync from unknown member. Current members: ${data.members}")
        sender() ! SyncReply(selfAddress, stateName, RevaultMemberStatus.Unknown, data.clusterHash)
        None
      }
    }
  }

  def addNewMember(member: Member, data: ProcessorData): Option[ProcessorData] = {
    if (member.hasRole("revault") && member.address != selfAddress) {
      val actor = context.actorSelection(RootActorPath(member.address) / "user" / "revault")
      val newData = data + member.address → RevaultMemberActor(
        actor, RevaultMemberStatus.Unknown, RevaultMemberStatus.Unknown
      )
      if (log.isDebugEnabled) {
        log.debug(s"New member: $member. State is unknown yet")
      }
      Some(newData)
    }
    else {
      None
    }
  }

  def removeMember(member: Member, data: ProcessorData): Option[ProcessorData] = {
    if (member.hasRole("revault")) {
      if (log.isDebugEnabled) {
        log.debug(s"Member removed: $member.")
      }
      Some(data - member.address)
    } else {
      None
    }
  }

  def processTask(task: Task, data: ProcessorData): Unit = {
    if (log.isDebugEnabled) {
      log.debug(s"Got task to process: $task")
    }
    if (task.content.isExpired) {
      log.warning(s"Task is expired, dropping: $task")
    } else {
      if (data.taskIsFor(task) == data.selfAddress) {
        if (data.taskWasFor(task) != data.selfAddress) {
          if (log.isDebugEnabled) {
            log.debug(s"Stashing task received for deactivating node: ${data.taskWasFor(task)}: $task")
          }
          stash()
        } else {
          activeWorkers.find(_._1.key == task.key) map { case (_, activeWorker) ⇒
            if (log.isDebugEnabled) {
              log.debug(s"Stashing task for the 'locked' URL: $task worker: $activeWorker")
            }
            stash()
            true
          } getOrElse {
            if (inactiveWorkers.isEmpty) {
              if (log.isDebugEnabled) {
                log.debug(s"No free worker, stashing task: $task")
              }
              stash()
            } else {
              val worker = inactiveWorkers.pop()
              if (log.isDebugEnabled) {
                log.debug(s"Forwarding to worker $worker task: $task")
              }
              worker ! task
              activeWorkers.append((task, worker))
            }
          }
        }
      }
      else {
        forwardTask(task, data)
      }
    }
  }

  def holdTask(task: Task, data: ProcessorData): Unit = {
    if (log.isDebugEnabled) {
      log.debug(s"Got task to process while activating: $task")
    }
    if (task.content.isExpired) {
      log.warning(s"Task is expired, dropping: $task")
    } else {
      if (data.taskIsFor(task) == data.selfAddress) {
        if (log.isDebugEnabled) {
          log.debug(s"Stashing task while activating: $task")
        }
        stash()
      } else {
        forwardTask(task, data)
      }
    }
  }

  def forwardTask(task: Task, data: ProcessorData): Unit = {
    val address = data.taskIsFor(task)
    data.members.get(address) map { rvm ⇒
      rvm.actorRef ! task
      true
    } getOrElse {
      log.error(s"Task actor is not found: $address, dropping: $task")
    }
  }

  def workerIsReadyForNextTask(): Unit = {
    val idx = activeWorkers.indexWhere(_._2 == sender())
    if (idx >= 0) {
      val (task,worker) = activeWorkers(idx)
      if (log.isDebugEnabled) {
        log.debug(s"Worker $worker is ready for next task. Completed task: $task")
      }
      activeWorkers.remove(idx)
      inactiveWorkers.push(worker)
      unstashAll()
    } else {
      log.error(s"workerIsReadyForNextTask: unknown worker actor: $sender")
    }
  }

  protected [this] implicit class ImplicitExtender(data: Option[ProcessorData]) {
    def andUpdate: State = {
      if (data.isDefined)
        stay using data.get
      else
        stay
    }
  }
}
