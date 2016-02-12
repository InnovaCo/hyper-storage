package eu.inn.revault

import java.io.ByteArrayInputStream

import akka.actor.{ActorLogging, Actor, ActorRef}
import akka.util.Timeout
import eu.inn.binders.dynamic.Text
import eu.inn.hyperbus.HyperBus
import eu.inn.hyperbus.akkaservice.AkkaHyperService
import eu.inn.hyperbus.model.{DynamicBody, Body, Response}
import eu.inn.hyperbus.model.standard._
import eu.inn.hyperbus.serialization.MessageDeserializer
import eu.inn.hyperbus.util.StringSerializer
import eu.inn.revault.protocol.{RevaultPut, RevaultGet}
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global // todo: inject
import akka.pattern.ask

class RevaultDistributor(revaultProcessor: ActorRef) extends Actor with ActorLogging {

  def receive = AkkaHyperService.dispatch(this)

  def ~> (implicit request: RevaultGet) = {
      Future {
        val result = DynamicBody(Text("Yey"))
        Ok(result)
      }
    }

  def ~> (implicit request: RevaultPut) = {

    val str = StringSerializer.serializeToString(request)
    val task = RevaultTask(request.path, System.currentTimeMillis() + 10000, str)
    implicit val timeout = Timeout(20.seconds)

    log.info(s"sending task from ${self} to ${revaultProcessor}. sender is: ${sender()}") // todo: remove
    revaultProcessor ? task map {
      case RevaultTaskResult(content) ⇒
        log.info(s"received: $content") //todo: remove
        response(content)
      case other ⇒
        log.error(s"Received: $other") // todo: eliminate
    }
  }

  // todo: shit method!
  def response(content: String): Response[Body] = {
    val byteStream = new ByteArrayInputStream(content.getBytes("UTF-8"))
    MessageDeserializer.deserializeResponseWith(byteStream) { (responseHeader, responseBodyJson) =>
      val body: Body = if (responseHeader.status >= 400) {
        ErrorBody(responseHeader.contentType, responseBodyJson)
      }
      else {
        DynamicBody(responseHeader.contentType, responseBodyJson)
      }
      StandardResponse(responseHeader, body)
    }
  }
}
