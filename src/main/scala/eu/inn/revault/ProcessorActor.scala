package eu.inn.revault

import akka.actor._
import akka.cluster.ClusterEvent.{MemberEvent, MemberRemoved, MemberUp}
import akka.cluster.{Cluster, ClusterEvent, Member}
import akka.routing.{MurmurHash, ConsistentHash}
import scala.concurrent.duration._
import scala.collection.mutable

case class ShardMessage(key: String, content: Any)

abstract class RevaultMemberStatus
object RevaultMemberStatus {
  @SerialVersionUID(1L) case object Unknown extends RevaultMemberStatus
  @SerialVersionUID(1L) case object Passive extends RevaultMemberStatus
  @SerialVersionUID(1L) case object Activating extends RevaultMemberStatus
  @SerialVersionUID(1L) case object Active extends RevaultMemberStatus
  @SerialVersionUID(1L) case object Deactivating extends RevaultMemberStatus
}

abstract class RevaultSyncResult
object RevaultSyncResult {
  @SerialVersionUID(1L) case object None extends RevaultSyncResult
  @SerialVersionUID(1L) case object Allowed extends RevaultSyncResult
  @SerialVersionUID(1L) case object NotFound extends RevaultSyncResult
}

case class Sync(applicantAddress: Address, status: RevaultMemberStatus, clusterHash: Int)
case class SyncReply(confirmerAddress: Address, status: RevaultMemberStatus, syncResult: RevaultSyncResult, clusterHash: Int)

case class RevaultMemberActor(actorRef: ActorSelection,
                              status: RevaultMemberStatus,
                              syncResult: RevaultSyncResult,
                              isSelf: Boolean)

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

/*

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
