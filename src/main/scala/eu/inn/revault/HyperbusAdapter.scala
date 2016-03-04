package eu.inn.revault

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.pattern.ask
import eu.inn.binders.dynamic.{Lst, Obj}
import eu.inn.hyperbus.akkaservice.AkkaHyperService
import eu.inn.revault.db.Db
import eu.inn.revault.protocol._
import eu.inn.hyperbus.serialization.{StringSerializer,StringDeserializer}
import eu.inn.hyperbus.model._
import scala.concurrent.duration._

class HyperbusAdapter(revaultProcessor: ActorRef, db: Db, requestTimeout: FiniteDuration) extends Actor with ActorLogging {
  import context._

  def receive = AkkaHyperService.dispatch(this)

  def ~> (implicit request: RevaultGet) = {
    val (documentUri, itemSegment) = ContentLogic.splitPath(request.path)
    if (itemSegment.isEmpty && request.body.pageFrom.isDefined) {

      val sortByDesc = request.body.sortBy.contains(SortBy("id",descending=true))

      val select = if (sortByDesc)
        db.selectContentCollectionDesc _
      else
        db.selectContentCollection _

      select(documentUri, request.body.pageSize.map(_.asInt).getOrElse(50)) map { collection ⇒ // todo:
        val stream = collection.toStream
        if (stream.isEmpty) {
          NotFound(ErrorBody("not_found", Some(s"Resource ${request.path} is not found")))
        }
        else {
          val result = Obj(Map("_embedded" →
            Obj(Map("els" →
            Lst(stream.filterNot(_.itemSegment.isEmpty).map { item ⇒
              StringDeserializer.dynamicBody(item.body).content
            }.toSeq)
          ))))

          Ok(DynamicBody(result), Headers(Map(Header.REVISION → Seq(stream.head.revision.toString))))
        }
      }
    }
    else {
      db.selectContent(documentUri, itemSegment) map {
        case None ⇒
          NotFound(ErrorBody("not_found", Some(s"Resource ${request.path} is not found")))
        case Some(content) ⇒
          val body = StringDeserializer.dynamicBody(content.body)
          Ok(body, Headers(Map(Header.REVISION → Seq(content.revision.toString))))
      }
    }
  }

  def ~> (request: RevaultPut) = executeRequest(request, request.path)
  def ~> (request: RevaultPost) = executeRequest(request, request.path)
  def ~> (request: RevaultPatch) = executeRequest(request, request.path)
  def ~> (request: RevaultDelete) = executeRequest(request, request.path)

  private def executeRequest(implicit request: Request[Body], uri: String) = {
    val str = StringSerializer.serializeToString(request)
    val ttl = Math.min(requestTimeout.toMillis - 100, 100)
    val (documentUri, _) = ContentLogic.splitPath(uri)
    val task = RevaultTask(documentUri, System.currentTimeMillis() + ttl, str)
    implicit val timeout: akka.util.Timeout = requestTimeout

    revaultProcessor ? task map {
      case RevaultTaskResult(content) ⇒
        StringDeserializer.dynamicResponse(content)
    }
  }
}
