package eu.inn.revault

import akka.actor._
import akka.cluster.ClusterEvent.{MemberEvent, MemberRemoved, MemberUp}
import akka.cluster.{Cluster, ClusterEvent, Member}
import akka.routing.{MurmurHash, ConsistentHash}
import scala.concurrent.duration._

case class ShardMessage(key: String, content: Any)

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

/*sealed trait ProcessorState
case object Starting extends ProcessorState
//case object Syncing extends ProcessorState
case object Active extends ProcessorState
case object Downing extends ProcessorState*/

case class ProcessorData(members: Map[Address, RevaultMemberActor], selfAddress: Address) {
  lazy val clusterHash = MurmurHash.stringHash(
      (members.keySet + selfAddress).map(_.toString
    ).toList.sorted.mkString("|"))
  def + (elem: (Address, RevaultMemberActor)) = ProcessorData(members + elem, selfAddress)
  def - (key: Address) = ProcessorData(members - key, selfAddress)
}

sealed trait Events
case object SyncTimer extends Events
case object ShutdownProcessor extends Events

class ProcessorFSM extends FSM[RevaultMemberStatus, ProcessorData] {
  private val cluster = Cluster(context.system)
  private val selfAddress = cluster.selfAddress
  cluster.subscribe(self, initialStateMode = ClusterEvent.InitialStateAsEvents, classOf[MemberEvent])

  startWith(RevaultMemberStatus.Activating, ProcessorData(Map.empty, selfAddress))

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
  }

  when(RevaultMemberStatus.Active) {
    case Event(SyncTimer, data) ⇒
      confirmStatus(data, RevaultMemberStatus.Active, isFirst = false)
      stay

    case Event(ShutdownProcessor, data) ⇒
      confirmStatus(data, RevaultMemberStatus.Deactivating, isFirst = true)
      goto(RevaultMemberStatus.Deactivating)
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

    case Event(e, s) =>
      log.warning("received unhandled request {} in state {}/{}", e, stateName, s)
      stay
  }

  initialize()

  def setSyncTimer(): Unit = {
    setTimer("syncing", SyncTimer, 1 second) // todo: move timeout to configuration
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
      log.info(s"ClusterHash ($data.clusterHash) is not matched for $sync")
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

  protected [this] implicit class ImplicitExtender(data: Option[ProcessorData]) {
    def andUpdate: State = {
      if (data.isDefined)
        stay using data.get
      else
        stay
    }
  }
}

/*
private [revault] case object SyncTimer

class ProcessorActor/*(workerProps: Props, workerCount: Int)*/ extends Actor with ActorLogging {
  val cluster = Cluster(context.system)
  /*val workers = for (workerIndex ← 1 to workerCount) {
    context.system.actorOf(workerProps, s"revault-wrkr-$workerIndex")
  }
  val inactiveWorkers = mutable.Stack(workers)*/
  cluster.subscribe(self, initialStateMode = ClusterEvent.InitialStateAsEvents, classOf[MemberEvent])

  val members = mutable.Map[Address, RevaultMemberActor]()
  var timerSet = false
  val activateSyncTimer = 5.seconds


  //var c = ConsistentHash(Seq[Member](), 100) // todo: find a better value for keys/rings count

  //c.nodeFor()

  def receive = {
    case MemberUp(member) ⇒
      if (member.hasRole("revault")) {
        val actor = context.actorSelection(RootActorPath(member.address) / "user" / "revault")
        members += member.address → RevaultMemberActor(
          actor, RevaultMemberStatus.Unknown, RevaultSyncResult.None, member.address == cluster.selfAddress
        )
        val sync = Sync(cluster.selfAddress, RevaultMemberStatus.Activating, clusterHash)
        actor ! sync
        enableTimer()
        if (log.isDebugEnabled) {
          log.debug(s"New member: $member. $sync was sent to $actor")
        }
      }

    case MemberRemoved(member, previousState) ⇒
      if (member.hasRole("revault")) {
        members -= member.address
        if (log.isDebugEnabled) {
          log.debug(s"Member removed: $member.")
        }
      }

    case sync: Sync ⇒
      if (log.isDebugEnabled) {
        log.debug(s"$sync received from $sender")
      }
      if (sync.clusterHash != clusterHash) {
        log.info(s"ClusterHash ($clusterHash) is not matched for $sync")
      } else {
        val syncReply =
          members.get(sync.applicantAddress) match {
            case Some(member) ⇒
              members += sync.applicantAddress →
                member.copy(status = sync.status)
              SyncReply(cluster.selfAddress,
                RevaultMemberStatus.Activating,
                RevaultSyncResult.Allowed,
                clusterHash
              )
            case None ⇒
              log.error(s"Got $sync from unknown member. Current members: $members")
              SyncReply(cluster.selfAddress,
                RevaultMemberStatus.Activating,
                RevaultSyncResult.NotFound,
                clusterHash
              )
          }
        sender() ! syncReply
      }

    case syncReply : SyncReply ⇒
      if (log.isDebugEnabled) {
        log.debug(s"$syncReply received from $sender")
      }
      if (syncReply.clusterHash != clusterHash) {
        log.info(s"ClusterHash ($clusterHash) is not matched for $syncReply")
      } else {
        members.get(syncReply.confirmerAddress) match {
          case Some(member) ⇒
            members += syncReply.confirmerAddress →
              member.copy(status = syncReply.status, syncResult = syncReply.syncResult)
          case None ⇒
            log.error(s"Got $syncReply from unknown member. Current members: $members")
        }
      }

    case SyncTimer ⇒
      if (updateStatus(RevaultMemberStatus.Activating, isFirst = false)) {
        updateStatus(RevaultMemberStatus.Active, isFirst = true)
        context.become(active)
      }
  }

  def active: Receive = {
    case SyncTimer ⇒
      updateStatus(RevaultMemberStatus.Active, isFirst = false)
  }

  def updateStatus(status: RevaultMemberStatus, isFirst: Boolean): Boolean = {
    var syncedWithAllMembers = true
    members.foreach { case (address, member) ⇒
      if (!member.isSelf && member.syncResult != RevaultSyncResult.Allowed) {
        syncedWithAllMembers = false
        val sync = Sync(cluster.selfAddress, status, clusterHash)
        member.actorRef ! sync
        enableTimer()
        if (isFirst && log.isDebugEnabled) {
          log.debug(s"Didn't received reply from: $member. $sync was sent to ${member.actorRef}")
        }
      }
    }
    syncedWithAllMembers && members.nonEmpty
  }

  // todo: cache hash?
  def clusterHash: Int = MurmurHash.stringHash(members.keys.map(_.toString).toList.sorted.mkString("|"))

  def enableTimer() = {
    if (!timerSet) {
      timerSet = true
      import context._
      system.scheduler.scheduleOnce(activateSyncTimer, context.self, SyncTimer)
    }
  }
}


1. start

  1.1. status = paused
  1.2. wait while cluster is up
  1.3. create hash ring
  1.4. start notifying each member that i'm ready to resume
  1.5. if got message from everyone, status = ready

2. shutdown (status = ready)
  2.1. status = pausing
  2.2. for each member:
    2.2.1. check or cancel that started operations that has to be owned by the member(processor) is complete
    2.2.2. notify a member about new ownership
  2.3. send poison kill to self
  2.4. forward incoming commands to the new owners

3. new node joined, node deleted, got message about changing ownership
  3.1. status = rebalancing?
  3.2. update member's
  3.3. for each member:
    3.3.1. check or cancel that started operations that has to be owned by the member(processor) is complete
    3.#.2. notify a member about new ownership
  3.3. status = ready

4. perform periodic checks that cluster status is OK, if not then rebalance

*/
