package mock

import com.typesafe.config.Config
import eu.inn.hyperbus.transport.api._

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

class FaultClientTransport(config: Config) extends ClientTransport {
  override def ask(message: TransportRequest, outputDeserializer: Deserializer[TransportResponse]): Future[TransportResponse] = {
    Future.failed(new RuntimeException("ask failed (test method)"))
  }

  override def publish(message: TransportRequest): Future[PublishResult] = Future {
    if (FaultClientTransport.checkers.exists { checker ⇒
      checker.isDefinedAt(message) && checker(message)
    }) {
      throw new RuntimeException("publish failed (test method)")
    }
    else {
      new PublishResult {
        override def sent: Option[Boolean] = None

        override def offset: Option[String] = None
      }
    }
  }

  override def shutdown(duration: FiniteDuration): Future[Boolean] = Future {
    true
  }
}

object FaultClientTransport {
  val checkers = mutable.ArrayBuffer[PartialFunction[TransportMessage, Boolean]]()
}
