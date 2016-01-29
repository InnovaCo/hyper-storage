import akka.actor.{PoisonPill, Props, ActorSystem}
import akka.util.Timeout
import com.typesafe.config.{ConfigFactory, Config}
import eu.inn.hyperbus.{HyperBus}
import eu.inn.hyperbus.model.standard.{EmptyBody, Ok}
import eu.inn.hyperbus.transport.ActorSystemRegistry
import eu.inn.hyperbus.transport.api.{TransportManager, TransportConfigurationLoader}
import eu.inn.revault.RevaultDistributor
import eu.inn.revault.protocol.RevaultGet
import org.scalatest.{BeforeAndAfterAll, Matchers, FreeSpec}
import org.scalatest.concurrent.ScalaFutures
import scala.concurrent.ExecutionContext.Implicits.global // todo: inject
import scala.concurrent.duration._
import eu.inn.hyperbus.akkaservice._

/*
class RevaultSpec extends FreeSpec with Matchers with ScalaFutures with BeforeAndAfterAll {
  var hyperBus: HyperBus = null
  implicit var actorSystem: ActorSystem = null
  override def beforeAll() {
    println("Before!")  // start up your web server or whatever
    val config = ConfigFactory.load()
    val transportConfiguration = TransportConfigurationLoader.fromConfig(config)
    val transportManager = new TransportManager(transportConfiguration)
    hyperBus = new HyperBus(transportManager)
    actorSystem = ActorSystemRegistry.get("eu-inn-cluster").get
  }

  override def afterAll() {
    hyperBus.shutdown(30.second)
    hyperBus = null
    println("After!")  // shut down the web server
  }

  "Revault" - {
    "Debug" in {
      val processor =
      val distributor = actorSystem.actorOf(Props(new RevaultDistributor(null)))
      implicit val timeout = Timeout(20.seconds)
      val subscriptions = hyperBus.routeTo[RevaultDistributor](distributor)

      try {
        val f = hyperBus <~ RevaultGet("abcde", EmptyBody)
        whenReady(f) { result ⇒
          result should equal(Ok(EmptyBody))
        }
      }
      finally {
        subscriptions.foreach(hyperBus.off)
        distributor ! PoisonPill
      }
    }

    "Debug2" in {
      true should equal(1)
    }
  }
}
*/
