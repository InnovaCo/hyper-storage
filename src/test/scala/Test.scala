import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorSystem, Actor, ActorLogging}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.event.LoggingReceive
import akka.testkit.{TestKit, TestFSMRef, TestActorRef}
import com.typesafe.config.ConfigFactory
import eu.inn.hyperbus.transport._
import eu.inn.hyperbus.transport.api._
import eu.inn.revault.{RevaultMemberStatus, ProcessorFSM}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FreeSpecLike, BeforeAndAfter, FreeSpec, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}

class TestActorX extends Actor with ActorLogging {
  val membersUp = new AtomicInteger(0)
  val memberUpPromise = Promise[Unit]()
  val memberUpFuture: Future[Unit] = memberUpPromise.future

  override def receive: Receive = LoggingReceive {
    case MemberUp(member) => {
      membersUp.incrementAndGet()
      memberUpPromise.success({})
      log.info("Member is ready!")
    }
  }
}

class Test(transportManager: TransportManager, implicit val actorSystem: ActorSystem)
  extends TestKit(actorSystem)
  with FreeSpecLike with ScalaFutures with Matchers with BeforeAndAfter {

  def this() = this(
    new TransportManager(TransportConfigurationLoader.fromConfig(ConfigFactory.load())),
    ActorSystemRegistry.get("eu-inn").get
  )

  val cluster = Cluster(actorSystem)
  val testActorX = TestActorRef[TestActorX]
  cluster.subscribe(testActorX, initialStateMode = InitialStateAsEvents, classOf[MemberEvent])

  after {
    Await.result(transportManager.shutdown(20.seconds), 20.seconds)
    cluster.unsubscribe(testActorX)
  }

  "TestProcessor " - {
    "ProcessorFSM should become Active when it's single member" in {
      val fsm = TestFSMRef(new ProcessorFSM)
      val processorFsm: TestActorRef[ProcessorFSM] = fsm

      fsm.stateName should equal(RevaultMemberStatus.Activating)
      awaitCond {
        fsm.stateName == RevaultMemberStatus.Active
      }
    }
  }
}
