import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorSystem, Actor, ActorLogging}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.event.LoggingReceive
import akka.testkit.TestActorRef
import com.typesafe.config.ConfigFactory
import eu.inn.hyperbus.transport._
import eu.inn.hyperbus.transport.api._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

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

class Test extends FreeSpec with ScalaFutures with Matchers with BeforeAndAfter {
  implicit var actorSystem: ActorSystem = null

  var transportManager: TransportManager = null
  before {
    val transportConfiguration = TransportConfigurationLoader.fromConfig(ConfigFactory.load())
    transportManager = new TransportManager(transportConfiguration)
    ActorSystemRegistry.get("eu-inn").foreach { as ⇒
      actorSystem = as
      val testActor = TestActorRef[TestActorX]
      Cluster(actorSystem).subscribe(testActor, initialStateMode = InitialStateAsEvents, classOf[MemberEvent])
    }
  }

  after {
    if (transportManager != null) {
      Await.result(transportManager.shutdown(20.seconds), 20.seconds)
    }
  }

  "Test " - {
    "1" in {
      val cnt = new AtomicInteger(0)
      cnt.get() should equal(0)
    }
  }
}
