package eu.inn.hyperstorage

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import eu.inn.binders.value.{Lst, Null, Obj, Value}
import eu.inn.hyperbus.akkaservice.AkkaHyperService
import eu.inn.hyperbus.model._
import eu.inn.hyperbus.model.utils.SortBy
import eu.inn.hyperbus.model.utils.Sort._
import eu.inn.hyperbus.serialization.{StringDeserializer, StringSerializer}
import eu.inn.hyperstorage.db._
import eu.inn.hyperstorage.metrics.Metrics
import eu.inn.metrics.MetricsTracker
import eu.inn.hyperstorage.api.{HyperStorageIndexSortItem, _}
import eu.inn.hyperstorage.indexing.{FieldFiltersExtractor, IndexLogic, OrderFieldsLogic}
import eu.inn.parser.{HEval, HParser}
import eu.inn.parser.ast.Expression

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Success
import scala.util.control.NonFatal

class HyperbusAdapter(hyperStorageProcessor: ActorRef, db: Db, tracker: MetricsTracker, requestTimeout: FiniteDuration) extends Actor with ActorLogging {

  import context._

  //val COLLECTION_TOKEN_FIELD_NAME = "from"
  val COLLECTION_FILTER_NAME = "filter"
  val COLLECTION_SIZE_FIELD_NAME = "size"
  val MAX_SKIPPED_ROWS = 10000
  val MAX_COLLECTION_SELECTS = 500
  val DEFAULT_PAGE_SIZE = 100

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

    val f = request.body.content.selectDynamic(COLLECTION_FILTER_NAME)
    val filter = if (f.asString == "") None else Some(f.asString)
    val sortBy = request.body.sortBy.getOrElse(Seq.empty)

    val indexDefsFuture = if (filter.isEmpty && sortBy.isEmpty) {
      Future.successful(Iterator.empty)
    } else {
      db.selectIndexDefs(resourcePath.documentUri)
    }

    val pageSize = request.body.content.selectDynamic(COLLECTION_SIZE_FIELD_NAME) match {
      case Null ⇒ DEFAULT_PAGE_SIZE
      case other: Value ⇒ other.asInt
    }
    // val selectResult = db.selectContentCollection(resourcePath.documentUri, pageSize, pageFrom, !sortByDesc)

    for {
      contentStatic ← db.selectContentStatic(resourcePath.documentUri)
      indexDefs ← indexDefsFuture
      collectionStream ← selectCollection(resourcePath.documentUri, indexDefs, filter, sortBy, pageSize)
    } yield {
      if (contentStatic.isDefined && contentStatic.forall(!_.isDeleted)) {
        val result = Obj(Map("_embedded" →
          Obj(Map("els" →
            Lst(collectionStream)
          ))))

        // todo: detect revision, is this possible for a index?
        Ok(DynamicBody(result), Headers(
          contentStat.headOption.map(h ⇒ Header.REVISION → Seq(h.revision.toString)).toMap
        ))
      }
      else {
        notFound
      }
    }
  }

  // todo: refactor this method
  private def selectCollection(documentUri: String,
                               indexDefs: Iterator[IndexDef],
                               queryFilter: Option[String],
                               querySortBy: Seq[SortBy], pageSize: Int): Future[Stream[Value]] = {

    val queryFilterExpression = queryFilter.map(HParser(_).get)

    val defIdSort = HyperStorageIndexSortItem("id", Some(HyperStorageIndexSortFieldType.DECIMAL), Some(HyperStorageIndexSortOrder.ASC))

    // todo: this should be cached, heavy operations here
    val sources = indexDefs.flatMap { indexDef ⇒
      if (indexDef.status == IndexDef.STATUS_NORMAL) Some {
        val filterAST = indexDef.filterBy.map(HParser(_).get)
        val indexSortBy = indexDef.sortBy.map(IndexLogic.deserializeSortByFields).getOrElse(Seq.empty) :+ defIdSort
        (IndexLogic.weighIndex(queryFilterExpression, querySortBy, filterAST, indexSortBy), indexSortBy, Some(indexDef))
      }
      else {
        None
      }
    }.toSeq :+
      (IndexLogic.weighIndex(queryFilterExpression, querySortBy, None, Seq(defIdSort)), Seq(defIdSort), None)

    val (weight,indexSortFields,indexDefOpt) = sources.reduceLeft((left,right) ⇒ if (left._1 > right._1) left else right)

    //val skipMax = Math.min(MAX_SKIPPED_ROWS, pageSize) + 1
    val ffe = new FieldFiltersExtractor(indexSortFields)
    val queryFilterFields = queryFilterExpression.map(ffe.extract).getOrElse(Seq.empty)
    //val m = queryFilterFields.map(_.name).toSet
    // todo: detect filter exact match

    // todo: s1 extract sort fields and order
    val ckFields = OrderFieldsLogic.extractIndexSortFields(querySortBy, indexSortFields)
    val sortMatchIsExact = ckFields.size == querySortBy.size
    val endOfTime = System.currentTimeMillis() + requestTimeout.toMillis

    if (sortMatchIsExact) {
      q2(documentUri, indexDefOpt, pageSize, queryFilterFields, ckFields, queryFilterExpression, indexSortFields.lastOption, None,0,endOfTime,0)
    }
    else {
      // todo: fullscan here
      q2(documentUri, indexDefOpt, pageSize, queryFilterFields, ckFields, queryFilterExpression, indexSortFields.lastOption, None,0,endOfTime,0)
    }
  }

  def q(documentUri: String,
        indexDefOpt: Option[IndexDef],
        limit: Int,
        filterFields: Seq[FieldFilter],
        ckFields: Seq[CkField],
        queryFilterExpression: Option[Expression]): Future[(Stream[Value], Int, Int, Option[Value])] = {

    val f: Future[Iterator[CollectionContent]] = indexDefOpt match {
      case None ⇒
        db.selectContentCollection(documentUri,
          limit,
          filterFields.find(_.name == "id").map(_.value.asString),
          ckFields.find(_.name == "id").forall(_.ascending)
        )

      case Some(indexDef) ⇒
        db.selectIndexCollection(
          indexDef.tableName,
          documentUri,
          indexDef.indexId,
          filterFields,
          ckFields,
          limit
        )
    }

    f.map { iterator ⇒
      var totalFetched = 0
      var totalAccepted = 0
      var lastValue: Option[Value] = None
      val acceptedStream = iterator.flatMap { c ⇒
        if (!c.itemId.isEmpty) {
          totalFetched += 1
          val v = StringDeserializer.dynamicBody(c.body).content
          lastValue = Some(v)
          val accepted = queryFilterExpression.forall { qfe ⇒
            v match {
              case o: Obj ⇒ try {
                new HEval(o).eval(qfe).asBoolean
              } catch {
                case NonFatal(e) ⇒ false
              }
              case _ ⇒ false
            }
          }
          if (accepted) {
            totalAccepted += 1
            Some(v)
          } else {
            None
          }
        } else {
          None
        }
      }.toStream

      (acceptedStream,totalAccepted,totalFetched,lastValue)
    }
  }

  def q2(documentUri: String,
         indexDefOpt: Option[IndexDef],
         limit: Int,
         filterFields: Seq[FieldFilter],
         ckFields: Seq[CkField],
         queryFilterExpression: Option[Expression],
         leastSortItem: Option[HyperStorageIndexSortItem],
         leastFieldFilter: Option[FieldFilter],
         recursionCounter: Int,
         endTimeInMillis: Long,
         skippedRows: Int
        ): Future[Stream[Value]] = {
    if (recursionCounter > MAX_COLLECTION_SELECTS)
      Future.failed(GatewayTimeout(ErrorBody("query-limit-reached", Some(s"Maximum query count is reached: $recursionCounter"))))
    else if (System.currentTimeMillis < endTimeInMillis)
      Future.failed(GatewayTimeout(ErrorBody("query-timed-out", Some(s"Timed out with query count: $recursionCounter"))))
    else if (skippedRows > MAX_SKIPPED_ROWS)
      Future.failed(GatewayTimeout(ErrorBody("query-skipped-rows-limit-reached", Some(s"Maximum skipped row limit is reached: $skippedRows"))))
    else {
      q(documentUri,indexDefOpt,limit,filterFields ++ leastFieldFilter,ckFields,queryFilterExpression) flatMap {
        case(stream,totalAccepted,totalFetched,lastValueOpt) ⇒
          if (totalAccepted >= limit || totalFetched < limit || leastSortItem.isEmpty) Future.successful(stream)
          else {
            val siv = lastValueOpt.flatMap { v ⇒
              IndexLogic.extractSortFieldValues(leastSortItem.toSeq, v).headOption
            }
            if (siv.isEmpty) Future.successful(stream) else {
              val op = if (leastSortItem.forall(_.order.forall(_ == HyperStorageIndexSortOrder.ASC))) FilterGt else FilterLt
              val nextLeastFieldFilter = FieldFilter(siv.get._1, siv.get._2, op)

              q2(documentUri, indexDefOpt, limit, filterFields, ckFields, queryFilterExpression, leastSortItem, Some(nextLeastFieldFilter), recursionCounter+1, endTimeInMillis, skippedRows + totalFetched - totalAccepted)
            }
          }
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
