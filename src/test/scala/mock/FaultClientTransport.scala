package mock

import com.typesafe.config.Config
import eu.inn.hyperbus.transport.api._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

class FaultClientTransport(config: Config) extends ClientTransport {
  override def ask[OUT <: TransportResponse](message: TransportRequest, outputDeserializer: Deserializer[OUT]): Future[OUT] = {
    Future.failed(new RuntimeException("ask failed (test method)"))
  }
  override def publish(message: TransportRequest): Future[PublishResult] = {
    Future.failed(new RuntimeException("publish failed (test method)"))
  }
  override def shutdown(duration: FiniteDuration): Future[Boolean] = Future{true}
}
