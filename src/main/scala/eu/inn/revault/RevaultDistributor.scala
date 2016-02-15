package eu.inn.revault

import java.io.ByteArrayInputStream

import akka.actor.{ActorLogging, Actor, ActorRef}
import akka.util.Timeout
import eu.inn.binders.dynamic.Text
import eu.inn.hyperbus.HyperBus
import eu.inn.hyperbus.akkaservice.AkkaHyperService
import eu.inn.hyperbus.model.serialization.util.StringDeserializer
import eu.inn.hyperbus.model.{DynamicBody, Body, Response}
import eu.inn.hyperbus.model.standard._
import eu.inn.hyperbus.serialization.MessageDeserializer
import eu.inn.hyperbus.util.StringSerializer
import eu.inn.revault.db.Db
import eu.inn.revault.protocol.{RevaultPut, RevaultGet}
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global // todo: inject
import akka.pattern.ask

class RevaultDistributor(revaultProcessor: ActorRef, db: Db) extends Actor with ActorLogging {

  def receive = AkkaHyperService.dispatch(this)

  def ~> (implicit request: RevaultGet) =  db.selectContent(request.path, "") map {
    case None ⇒ NotFound(ErrorBody("not_found", Some(s"Resource ${request.path} is not found")))
    case Some(content) ⇒
      val body = StringDeserializer.dynamicBody(content.body)
      Ok(body, Map("hyperbus:revision" → Seq(content.revision.toString)))
  }

  def ~> (implicit request: RevaultPut) = {

    val str = StringSerializer.serializeToString(request)
    val task = RevaultTask(request.path, System.currentTimeMillis() + 10000, str)
    implicit val timeout = Timeout(20.seconds)

    revaultProcessor ? task map {
      case RevaultTaskResult(content) ⇒
        StringDeserializer.dynamicResponse(content)
      case other ⇒
    }
  }
}
