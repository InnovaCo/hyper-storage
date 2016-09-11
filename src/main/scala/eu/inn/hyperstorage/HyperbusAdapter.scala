package eu.inn.hyperstorage

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import eu.inn.binders.value.{Lst, Null, Number, Obj, Value}
import eu.inn.hyperbus.akkaservice.AkkaHyperService
import eu.inn.hyperbus.model._
import eu.inn.hyperbus.model.utils.Sort._
import eu.inn.hyperbus.model.utils.SortBy
import eu.inn.hyperbus.serialization.{StringDeserializer, StringSerializer}
import eu.inn.hyperstorage.api.{HyperStorageIndexSortItem, _}
import eu.inn.hyperstorage.db._
import eu.inn.hyperstorage.indexing.{FieldFiltersExtractor, IndexLogic, OrderFieldsLogic}
import eu.inn.hyperstorage.metrics.Metrics
import eu.inn.hyperstorage.workers.primary.{PrimaryTask, PrimaryWorkerTaskResult}
import eu.inn.hyperstorage.workers.secondary.IndexDefTask
import eu.inn.metrics.MetricsTracker
import eu.inn.parser.ast.{Expression, Identifier}
import eu.inn.parser.eval.ValueContext
import eu.inn.parser.{HEval, HParser}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

class HyperbusAdapter(hyperStorageProcessor: ActorRef, db: Db, tracker: MetricsTracker, requestTimeout: FiniteDuration) extends Actor with ActorLogging {

  import context._

  final val COLLECTION_FILTER_NAME = "filter"
  final val COLLECTION_SIZE_FIELD_NAME = "size"
  final val COLLECTION_SKIP_MAX_FIELD_NAME = "skipMax"
  final val DEFAULT_MAX_SKIPPED_ROWS = 10000
  final val MAX_COLLECTION_SELECTS = 20
  final val DEFAULT_PAGE_SIZE = 100

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
    val task = PrimaryTask(documentUri, System.currentTimeMillis() + ttl, str)
    implicit val timeout: akka.util.Timeout = requestTimeout

    // todo: what happens when error is returned
    hyperStorageProcessor ? task map {
      case PrimaryWorkerTaskResult(content) ⇒
        StringDeserializer.dynamicResponse(content)
    }
  }

  def ~>(request: HyperStorageContentPatch) = executeRequest(request, request.path)

  def ~>(request: HyperStorageContentDelete) = executeRequest(request, request.path)

  def ~>(request: HyperStorageIndexPost) = executeIndexRequest(request)

  def ~>(request: HyperStorageIndexDelete) = executeIndexRequest(request)

  private def executeIndexRequest(request: Request[Body]) = {
    val ttl = Math.max(requestTimeout.toMillis - 100, 100)
    val key = request match {
      case post: HyperStorageIndexPost ⇒ post.path
      case delete: HyperStorageIndexDelete ⇒ delete.path
    }
    val indexDefTask = IndexDefTask(System.currentTimeMillis() + ttl, key, StringSerializer.serializeToString(request))
    implicit val timeout: akka.util.Timeout = requestTimeout

    hyperStorageProcessor ? indexDefTask map {
      case r: Response[Body] ⇒
        r
    }
  }

  private def queryCollection(resourcePath: ResourcePath, request: HyperStorageContentGet) = {
    implicit val mcx = request
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

    val skipMax = request.body.content.selectDynamic(COLLECTION_SKIP_MAX_FIELD_NAME) match {
      case Null ⇒ DEFAULT_MAX_SKIPPED_ROWS
      case other: Value ⇒ other.asInt
    }

    for {
      contentStatic ← db.selectContentStatic(resourcePath.documentUri)
      indexDefs ← indexDefsFuture
      (collectionStream,revisionOpt) ← selectCollection(resourcePath.documentUri, indexDefs, filter, sortBy, pageSize, skipMax)
    } yield {
      if (contentStatic.isDefined && contentStatic.forall(!_.isDeleted)) {
        val result = Obj(Map("_embedded" →
          Obj(Map("els" →
            Lst(collectionStream)
          ))))

        Ok(DynamicBody(result), Headers(
          revisionOpt.map(r ⇒ Header.REVISION → Seq(r.toString)).toMap
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
                               querySortBy: Seq[SortBy],
                               pageSize: Int,
                               skipMax: Int): Future[(List[Value], Option[Long])] = {

    val queryFilterExpression = queryFilter.flatMap(Option.apply).map(HParser(_).get)

    val defIdSort = HyperStorageIndexSortItem("id", Some(HyperStorageIndexSortFieldType.TEXT), Some(HyperStorageIndexSortOrder.ASC))

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

    val ffe = new FieldFiltersExtractor(indexSortFields)
    val queryFilterFields = queryFilterExpression.map(ffe.extract).getOrElse(Seq.empty)
    // todo: detect filter exact match

    val (ckFields,reversed) = OrderFieldsLogic.extractIndexSortFields(querySortBy, indexSortFields)
    val sortMatchIsExact = ckFields.size == querySortBy.size || querySortBy.isEmpty
    val endOfTime = System.currentTimeMillis + requestTimeout.toMillis

    if (sortMatchIsExact) {
      queryUntilFetched(
        CollectionQueryOptions(documentUri, indexDefOpt, indexSortFields, reversed, pageSize, skipMax, endOfTime, queryFilterFields, ckFields, queryFilterExpression, sortMatchIsExact),
        Seq.empty,0,0,None
      )  map { case (list, revisionOpt) ⇒
        (list.take(pageSize), revisionOpt)
      }
    }
    else {
      queryUntilFetched(
        CollectionQueryOptions(documentUri, indexDefOpt, indexSortFields, reversed, pageSize + skipMax, pageSize + skipMax, endOfTime, queryFilterFields, ckFields, queryFilterExpression, sortMatchIsExact),
        Seq.empty,0,0,None
      ) map { case (list, revisionOpt) ⇒
        if (list.size==(pageSize+skipMax)) {
          throw GatewayTimeout(ErrorBody("query-skipped-rows-limited", Some(s"Maximum skipped row limit is reached: $skipMax")))
        } else {
          if (querySortBy.nonEmpty) {
            implicit val ordering = new CollectionOrdering(querySortBy)
            (list.sorted.take(pageSize), revisionOpt)
          }
          else
            (list.take(pageSize), revisionOpt)
        }
      }
    }
  }

  private def queryAndFilterRows(ops: CollectionQueryOptions): Future[(List[Value], Int, Int, Option[Obj], Option[Long])] = {

    val f: Future[Iterator[CollectionContent]] = ops.indexDefOpt match {
      case None ⇒
        db.selectContentCollection(ops.documentUri,
          ops.limit,
          ops.filterFields.find(_.name == "item_id").map(ff ⇒ (ff.value.asString, ff.op)),
          ops.ckFields.find(_.name == "item_id").forall(_.ascending)
        )

      case Some(indexDef) ⇒
        db.selectIndexCollection(
          indexDef.tableName,
          ops.documentUri,
          indexDef.indexId,
          ops.filterFields,
          ops.ckFields,
          ops.limit
        )
    }

    f.map { iterator ⇒
      var totalFetched = 0
      var totalAccepted = 0
      var lastValue: Option[Obj] = None
      var revision: Option[Long] = None

      val acceptedStream = iterator.flatMap { c ⇒
        if (!c.itemId.isEmpty) {
          totalFetched += 1
          val optVal = StringDeserializer.dynamicBody(c.body).content match {
            case o: Obj ⇒ Some(o)
            case _ ⇒ None
          }

          val accepted = optVal map { o ⇒
            lastValue = Some(o)
            ops.queryFilterExpression.forall { qfe ⇒
              try {
                new HEval(o).eval(qfe).asBoolean
              } catch {
                case NonFatal(e) ⇒ false
              }
            }
          } getOrElse {
            false
          }

          if (accepted) {
            totalAccepted += 1
            if (revision.isEmpty) {
              revision = Some(c.revision)
            }
            optVal
          } else {
            None
          }
        } else {
          // record was returned, however there was no item, only static part
          // sometimes cassandra do this
          totalFetched += 1
          None
        }
      }.toList
      (acceptedStream, totalAccepted, totalFetched, lastValue, revision)
    }
  }

  private def queryUntilFetched(ops: CollectionQueryOptions,
                                leastFieldFilter: Seq[FieldFilter],
                                recursionCounter: Int,
                                skippedRows: Int,
                                lastValueOpt: Option[Obj]
        ): Future[(List[Value], Option[Long])] = {

    //println(s"queryUntilFetched($ops,$leastFieldFilter,$recursionCounter,$skippedRows,$lastValueOpt)")

  //todo exception context
    if (recursionCounter >= MAX_COLLECTION_SELECTS)
      Future.failed(GatewayTimeout(ErrorBody("query-count-limited", Some(s"Maximum query count is reached: $recursionCounter"))))
    else if (ops.endTimeInMillis <= System.currentTimeMillis)
      Future.failed(GatewayTimeout(ErrorBody("query-timeout", Some(s"Timed out performing query #$recursionCounter"))))
    else if (skippedRows >= ops.skipRowsLimit)
      Future.failed(GatewayTimeout(ErrorBody("query-skipped-rows-limited", Some(s"Maximum skipped row limit is reached: $skippedRows"))))
    else {
      val fetchLimit = ops.limit + Math.max((recursionCounter * (ops.skipRowsLimit - ops.limit)/(MAX_COLLECTION_SELECTS*1.0)).toInt, 0)
      queryAndFilterRows(ops.copy(filterFields=IndexLogic.mergeLeastQueryFilterFields(ops.filterFields, leastFieldFilter),limit=fetchLimit)) flatMap {
        case(stream,totalAccepted,totalFetched,newLastValueOpt,revisionOpt) ⇒
          val taken = stream.take(ops.limit)
          if (totalAccepted >= ops.limit ||
            ((leastFieldFilter.isEmpty || (leastFieldFilter.size==1 && leastFieldFilter.head.op != FilterEq)) && totalFetched < fetchLimit)) {
            Future.successful((stream, revisionOpt))
          }
          else {
            val l = newLastValueOpt.orElse(lastValueOpt)
            if (l.isEmpty) Future.successful((stream, revisionOpt))
            else {
              val nextLeastFieldFilter = IndexLogic.leastRowsFilterFields(ops.indexSortBy, ops.filterFields, leastFieldFilter.size, totalFetched < fetchLimit, l.get, ops.reversed)
              if (nextLeastFieldFilter.isEmpty) Future.successful((stream, revisionOpt))
              else {
                queryUntilFetched(ops, nextLeastFieldFilter, recursionCounter + 1, skippedRows + totalFetched - totalAccepted, l) map {
                  case (newStream, newRevisionOpt) ⇒
                    (stream ++ newStream, revisionOpt.flatMap(a ⇒ newRevisionOpt.map(b ⇒ Math.min(a, b))))
                }
              }
            }
          }
      }
    }
  }

  private def queryDocument(resourcePath: ResourcePath, request: HyperStorageContentGet) = {
    implicit val mcx = request
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

case class CollectionQueryOptions(documentUri: String,
                                  indexDefOpt: Option[IndexDef],
                                  indexSortBy: Seq[HyperStorageIndexSortItem],
                                  reversed: Boolean,
                                  limit: Int,
                                  skipRowsLimit: Int,
                                  endTimeInMillis: Long,
                                  filterFields: Seq[FieldFilter],
                                  ckFields: Seq[CkField],
                                  queryFilterExpression: Option[Expression],
                                  exactSortMatch: Boolean
                                 )

class CollectionOrdering(querySortBy: Seq[SortBy]) extends Ordering[Value] {
  private val sortIdentifiersStream = querySortBy.map { sb ⇒
    new HParser(sb.fieldName).Ident.run().get → sb.descending
  }.toStream


  override def compare(x: Value, y: Value): Int = {
    if (querySortBy.isEmpty) throw new UnsupportedOperationException("sort fields are required to compare collection items") else {
      sortIdentifiersStream.map { case (identifier,descending) ⇒
        val xv = extract(x, identifier)
        val yv = extract(y, identifier)
        if (descending)
          cmp(yv,xv)
        else
          cmp(xv,yv)
      }.find(_ != 0).getOrElse(0)
    }
  }

  private def extract(v: Value, identifier: Identifier): Value = {
    val valueContext = v match {
      case obj: Obj ⇒ ValueContext(obj)
      case _ ⇒ ValueContext(Obj.empty)
    }
    valueContext.identifier.applyOrElse(identifier, emptyValue)
  }

  private def emptyValue(i: Identifier) = Null

  private def cmp(x: Value, y: Value): Int = {
    (x,y) match {
      case (Number(xn),Number(yn)) ⇒ xn.compare(yn)
      case (xs,ys) ⇒ xs.asString.compareTo(ys.asString)
    }
  }
}

object HyperbusAdapter {
  def props(hyperStorageProcessor: ActorRef, db: Db, tracker: MetricsTracker, requestTimeout: FiniteDuration) = Props(
    classOf[HyperbusAdapter],
    hyperStorageProcessor, db, tracker, requestTimeout
  )
}
