package eu.inn.hyperstorage

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import eu.inn.binders.value.{Lst, Obj}
import eu.inn.hyperbus.akkaservice.AkkaHyperService
import eu.inn.hyperbus.model._
import eu.inn.hyperbus.model.utils.SortBy
import eu.inn.hyperbus.model.utils.Sort._
import eu.inn.hyperbus.serialization.{StringDeserializer, StringSerializer}
import eu.inn.hyperstorage.db.Db
import eu.inn.hyperstorage.metrics.Metrics
import eu.inn.metrics.MetricsTracker
import eu.inn.hyperstorage.api._

import scala.concurrent.duration._

class HyperbusAdapter(hyperStorageProcessor: ActorRef, db: Db, tracker: MetricsTracker, requestTimeout: FiniteDuration) extends Actor with ActorLogging {

  import context._

  val COLLECTION_TOKEN_FIELD_NAME = "from"
  val COLLECTION_SIZE_FIELD_NAME = "size"
  val MAX_SKIPPED_ROWS = 10000

  def receive = AkkaHyperService.dispatch(this)

  def ~>(implicit request: HyperStorageContentGet) = {
    tracker.timeOfFuture(Metrics.RETRIEVE_TIME) {
      val resourcePath = ContentLogic.splitPath(request.path)
      val notFound = NotFound(ErrorBody("not_found", Some(s"Resource '${request.path}' is not found")))
      if (ContentLogic.isCollectionUri(resourcePath.documentUri) && resourcePath.itemId.isEmpty) {
        queryCollection(resourcePath, request)
      }
      else {
        queryDocument(resourcePath, request)
      }
    }
  }

  def ~>(request: HyperStorageContentPut) = executeRequest(request, request.path)

  def ~>(request: HyperStorageContentPost) = executeRequest(request, request.path)

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

  def ~>(request: HyperStorageContentPatch) = executeRequest(request, request.path)

  def ~>(request: HyperStorageContentDelete) = executeRequest(request, request.path)

  def ~>(request: HyperStorageIndexPost) = executeIndexRequest(request)

  def ~>(request: HyperStorageIndexDelete) = executeIndexRequest(request)

  private def executeIndexRequest(request: Request[Body]) = {
    val ttl = Math.max(requestTimeout.toMillis - 100, 100)
    val indexDefTask = IndexDefTask(System.currentTimeMillis() + ttl, request)
    implicit val timeout: akka.util.Timeout = requestTimeout

    hyperStorageProcessor ? indexDefTask map {
      case r: Response[Body] ⇒
        r
    }
  }

  private def queryCollection(resourcePath: ResourcePath, request: HyperStorageContentGet) = {
    val notFound = NotFound(ErrorBody("not_found", Some(s"Resource '${request.path}' is not found")))
    // collection
    val sortByDesc = request.body.sortBy.exists(_.contains(SortBy("id", descending = true)))
    val pageFrom = request.body.content.asMap.get(COLLECTION_TOKEN_FIELD_NAME).map(_.asString)
    val pageSize = request.body.content.asMap(COLLECTION_SIZE_FIELD_NAME).asInt
    // (if (pageFrom.isEmpty && !sortByDesc) 1 else 0) + request.body.pageSize.map(_.asInt).getOrElse(50)

    val selectResult = db.selectContentCollection(resourcePath.documentUri, pageSize, pageFrom, !sortByDesc)

    for {
      contentStatic ← db.selectContentStatic(resourcePath.documentUri)
      collection ← selectResult
    } yield {
      if (contentStatic.isDefined) {
        val stream = collection.toStream
        val result = Obj(Map("_embedded" →
          Obj(Map("els" →
            Lst(stream.filterNot(s ⇒ s.itemId.isEmpty || s.isDeleted).map { item ⇒
              StringDeserializer.dynamicBody(item.body).content
            })
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

  private def queryDocument(resourcePath: ResourcePath, request: HyperStorageContentGet) = {
    val notFound = NotFound(ErrorBody("not_found", Some(s"Resource '${request.path}' is not found")))
    db.selectContent(resourcePath.documentUri, resourcePath.itemId) map {
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

object HyperbusAdapter {
  def props(hyperStorageProcessor: ActorRef, db: Db, tracker: MetricsTracker, requestTimeout: FiniteDuration) = Props(
    classOf[HyperbusAdapter],
    hyperStorageProcessor, db, tracker, requestTimeout
  )
}
