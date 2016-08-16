package eu.inn.hyperstorage

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import eu.inn.binders.value.{Lst, Number, Obj}
import eu.inn.hyperbus.akkaservice.AkkaHyperService
import eu.inn.hyperbus.model._
import eu.inn.hyperbus.model.utils.{Sort, SortBy}
import eu.inn.hyperbus.serialization.{StringDeserializer, StringSerializer}
import eu.inn.metrics.MetricsTracker
import eu.inn.hyperstorage.api._
import eu.inn.hyperstorage.db.Db
import eu.inn.hyperstorage.metrics.Metrics

import scala.concurrent.duration._

class HyperbusAdapter(hyperStorageProcessor: ActorRef, db: Db, tracker: MetricsTracker, requestTimeout: FiniteDuration) extends Actor with ActorLogging {
  import context._

  val COLLECTION_TOKEN_FIELD_NAME = "from"
  val COLLECTION_SIZE_FIELD_NAME = "size"

  def receive = AkkaHyperService.dispatch(this)

  def ~> (implicit request: HyperStorageContentGet) = {
    tracker.timeOfFuture(Metrics.RETRIEVE_TIME) {
      val ResourcePath(documentUri, itemSegment) = ContentLogic.splitPath(request.path)
      val notFound = NotFound(ErrorBody("not_found", Some(s"Resource '${request.path}' is not found")))
      if (itemSegment.isEmpty && request.body.content.asMap.contains(COLLECTION_SIZE_FIELD_NAME)) { // collection
        import Sort._

        val sortByDesc = request.body.sortBy.exists(_.contains(SortBy("id", descending = true)))
        val pageFrom = request.body.content.asMap.get(COLLECTION_TOKEN_FIELD_NAME).map(_.asString)
        val pageSize = request.body.content.asMap(COLLECTION_SIZE_FIELD_NAME).asInt
        // (if (pageFrom.isEmpty && !sortByDesc) 1 else 0) + request.body.pageSize.map(_.asInt).getOrElse(50)

        val selectResult = if (sortByDesc) {
          if (pageFrom.isEmpty)
            db.selectContentCollectionDesc(documentUri, pageSize)
          else
            db.selectContentCollectionDescFrom(documentUri, pageFrom.get, pageSize)
        }
        else {
          if (pageFrom.isEmpty)
            db.selectContentCollection(documentUri, pageSize)
          else
            db.selectContentCollectionFrom(documentUri, pageFrom.get, pageSize)
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

  def ~> (request: HyperStorageContentPut) = executeRequest(request, request.path)
  def ~> (request: HyperStorageContentPost) = executeRequest(request, request.path)
  def ~> (request: HyperStorageContentPatch) = executeRequest(request, request.path)
  def ~> (request: HyperStorageContentDelete) = executeRequest(request, request.path)

  def ~> (request: HyperStorageIndexPost) = {
    val ttl = Math.max(requestTimeout.toMillis - 100, 100)
    val indexDefTask = IndexDefTask(System.currentTimeMillis() + ttl, request)
    implicit val timeout: akka.util.Timeout = requestTimeout

    hyperStorageProcessor ? indexDefTask map {
      case r: Response[Body] ⇒
        r
    }
  }

  private def executeRequest(implicit request: Request[Body], uri: String) = {
    val str = StringSerializer.serializeToString(request)
    val ttl = Math.max(requestTimeout.toMillis - 100, 100)
    val documentUri = ContentLogic.splitPath(uri).documentUri
    val task = ForegroundTask(documentUri, System.currentTimeMillis() + ttl, str)
    implicit val timeout: akka.util.Timeout = requestTimeout

    hyperStorageProcessor ? task map {
      case ForegroundWorkerTaskResult(content) ⇒
        StringDeserializer.dynamicResponse(content)
    }
  }
}

object HyperbusAdapter {
  def props(hyperStorageProcessor: ActorRef, db: Db, tracker: MetricsTracker, requestTimeout: FiniteDuration) = Props(
    classOf[HyperbusAdapter],
    hyperStorageProcessor, db, tracker, requestTimeout
  )
}
