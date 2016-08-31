import ch.qos.logback.classic.{Level, Logger}
import eu.inn.binders.value.{Number, ObjV, Value}
import eu.inn.hyperbus.model.utils.SortBy
import eu.inn.hyperbus.model.{DynamicBody, QueryBuilder, Status}
import eu.inn.hyperstorage.api.{HyperStorageContentGet, HyperStorageContentPut}
import org.scalatest.{FreeSpec, Ignore}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalameter._
import org.slf4j.LoggerFactory

import scala.util.Random

@Ignore class BenchmarkTest extends FreeSpec
  with CassandraFixture
  with TestHelpers
  with Eventually {

  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(10000, Millis)))

  beforeAll()
  val hyperbus = integratedHyperbus(db)
  val random = new Random(100500)

  "BenchmarkTest" - {

    val colname = "collection-bench~"

    def nextRandomObj() = ObjV(
      "a" → random.nextInt(),
      "b" → random.alphanumeric.take(32).mkString,
      "c" → random.alphanumeric.take(10 + random.nextInt(100)).mkString,
      "d" → random.nextDouble()
    )

    def insert(id: String, content: Value) = {
      val f = hyperbus <~ HyperStorageContentPut(s"$colname/$id", DynamicBody(content))
      f.futureValue.statusCode shouldBe Status.CREATED
    }

    def query(sort: Seq[SortBy] = Seq.empty, filter: Option[String] = None, pageSize: Int = 50) = {
      import eu.inn.hyperbus.model.utils.Sort._
      val qb = new QueryBuilder() sortBy sort add("size", pageSize)
      filter.foreach(qb.add("filter", _))

      val res = (hyperbus <~ HyperStorageContentGet(colname,
        body = qb.result()
      )).futureValue
    }

    "Measure" in {
      val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger]
      root.setLevel(Level.INFO)

      val itemCount = 100
      println("Inserting!")
      val insertTime = measure {
        0 to itemCount map (_ ⇒ insert(random.alphanumeric.take(26).mkString, nextRandomObj()))
      }
      println(s"Total time to insert $itemCount items: $insertTime")

      val queryCount = 1000
      val queryTime = measure {
        0 to queryCount map (_ ⇒
            query()
          )
      }

      println(s"Total time to query $itemCount items: $queryTime")
    }
  }
}
