package eu.inn.revault

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import eu.inn.binders.value.{Lst, Obj}
import eu.inn.hyperbus.akkaservice.AkkaHyperService
import eu.inn.revault.db.Db
import eu.inn.revault.api._
import eu.inn.hyperbus.serialization.{StringDeserializer, StringSerializer}
import eu.inn.hyperbus.model._
import eu.inn.metrics.MetricsTracker
import eu.inn.revault.metrics.Metrics

import scala.concurrent.duration._

class HyperbusAdapter(revaultProcessor: ActorRef, db: Db, tracker: MetricsTracker, requestTimeout: FiniteDuration) extends Actor with ActorLogging {
  import context._

  def receive = AkkaHyperService.dispatch(this)

  def ~> (implicit request: RevaultContentGet) = {
    tracker.timeOfFuture(Metrics.RETRIEVE_TIME) {
      val (documentUri, itemSegment) = ContentLogic.splitPath(request.path)
      val notFound = NotFound(ErrorBody("not_found", Some(s"Resource '${request.path}' is not found")))
      if (itemSegment.isEmpty && request.body.pageFrom.isDefined) {
        val sortByDesc = request.body.sortBy.contains(SortBy("id", descending = true))
        val pageFrom = request.body.pageFrom.get.asString
        val pageSize = /*(if (pageFrom.isEmpty && !sortByDesc) 1 else 0) +*/ request.body.pageSize.map(_.asInt).getOrElse(50)


        val selectResult = if (sortByDesc) {
          if (pageFrom.isEmpty)
            db.selectContentCollectionDesc(documentUri, pageSize)
          else
            db.selectContentCollectionDescFrom(documentUri, pageFrom, pageSize)
        }
        else {
          if (pageFrom.isEmpty)
            db.selectContentCollection(documentUri, pageSize)
          else
            db.selectContentCollectionFrom(documentUri, pageFrom, pageSize)
        }

        for {
          contentStatic ← db.selectContentStatic(documentUri)
          collection ← selectResult
        } yield {
          // todo: 404 if no parent resource?
          if (contentStatic.isDefined) {
            val stream = collection.toStream
            val result = Obj(Map("_embedded" →
              Obj(Map("els" →
                Lst(stream.filterNot(s ⇒ s.itemSegment.isEmpty || s.isDeleted).map { item ⇒ // todo: isDeleted & paging = :(
                  StringDeserializer.dynamicBody(item.body).content
                }.toSeq)
              ))))

            Ok(DynamicBody(result), Headers(
              stream.headOption.map(h ⇒ Header.REVISION → Seq(h.revision.toString)).toMap
            ))
          }
          else {
            notFound
          }
        }
      }
      else {
        db.selectContent(documentUri, itemSegment) map {
          case None ⇒
            notFound
          case Some(content) ⇒
            if (!content.isDeleted) {
              val body = StringDeserializer.dynamicBody(content.body)
              Ok(body, Headers(Map(Header.REVISION → Seq(content.revision.toString))))
            } else {
              notFound
            }
        }
      }
    }
  }

  def ~> (request: RevaultContentPut) = executeRequest(request, request.path)
  def ~> (request: RevaultContentPost) = executeRequest(request, request.path)
  def ~> (request: RevaultContentPatch) = executeRequest(request, request.path)
  def ~> (request: RevaultContentDelete) = executeRequest(request, request.path)

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

object HyperbusAdapter {
  def props(revaultProcessor: ActorRef, db: Db, tracker: MetricsTracker, requestTimeout: FiniteDuration) = Props(
    classOf[HyperbusAdapter],
    revaultProcessor, db, tracker, requestTimeout
  )
}
