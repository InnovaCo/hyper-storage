package eu.inn.revault

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.pattern.ask
import akka.util.Timeout
import eu.inn.hyperbus.akkaservice.AkkaHyperService
import eu.inn.hyperbus.model.{Body, Request}
import eu.inn.hyperbus.model.serialization.util.StringDeserializer
import eu.inn.hyperbus.model.standard._
import eu.inn.hyperbus.util.StringSerializer
import eu.inn.revault.db.Db
import eu.inn.revault.protocol.{RevaultDelete, RevaultPatch, RevaultGet, RevaultPut}
import scala.concurrent.duration._

class RevaultDistributor(revaultProcessor: ActorRef, db: Db, requestTimeout: FiniteDuration) extends Actor with ActorLogging {
  import context._

  def receive = AkkaHyperService.dispatch(this)

  def ~> (implicit request: RevaultGet) = db.selectContent(request.path, "") map {
    case None ⇒ NotFound(ErrorBody("not_found", Some(s"Resource ${request.path} is not found")))
    case Some(content) ⇒
      val body = StringDeserializer.dynamicBody(content.body)
      Ok(body, Map("hyperbus:revision" → Seq(content.revision.toString)))
  }

  def ~> (request: RevaultPut) = executeRequest(request, request.path)
  def ~> (request: RevaultPatch) = executeRequest(request, request.path)
  def ~> (request: RevaultDelete) = executeRequest(request, request.path)

  private def executeRequest(implicit request: Request[Body], path: String) = {
    val str = StringSerializer.serializeToString(request)
    val ttl = Math.min(requestTimeout.toMillis - 100, 100)
    val task = RevaultTask(path, System.currentTimeMillis() + ttl, str)
    implicit val timeout: akka.util.Timeout = requestTimeout

    revaultProcessor ? task map {
      case RevaultTaskResult(content) ⇒
        StringDeserializer.dynamicResponse(content)
    }
  }
}
