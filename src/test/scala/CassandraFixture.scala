import com.datastax.driver.core.Session
import eu.inn.binders.cassandra.GuavaSessionQueryCache
import eu.inn.binders.naming.CamelCaseToSnakeCaseConverter
import eu.inn.hyperstorage.CassandraConnector
import eu.inn.hyperstorage.db.Db
import org.cassandraunit.CassandraCQLUnit
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet
import org.mockito.Mockito._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

trait CassandraFixture extends BeforeAndAfterAll with ScalaFutures {
  this: Suite =>
  private[this] val log = LoggerFactory.getLogger(getClass)
  //var session: Session = _
  var db: Db = _
  var dbOriginal: Db = _
  implicit var sessionQueryCache: GuavaSessionQueryCache[CamelCaseToSnakeCaseConverter] = _

  implicit def executionContext: ExecutionContext

  override def beforeAll() {
    Cassandra.start
    val connector = new CassandraConnector {
      override def connect(): Session = Cassandra.session
    }
    dbOriginal = new Db(connector)
    db = spy(dbOriginal)
    sessionQueryCache = new GuavaSessionQueryCache[CamelCaseToSnakeCaseConverter](Cassandra.session)
  }

  override def afterAll() {
    db = null
    dbOriginal = null
    sessionQueryCache = null
    //EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
  }

  def cleanUpCassandra(): Unit = {
    log.info("------- CLEANING UP C* -------- ")
    if (Cassandra.session != null) {
      import scala.collection.JavaConversions._
      val cleanDs = new ClassPathCQLDataSet("cleanup.cql", "hyper_storage_test")
      cleanDs.getCQLStatements.foreach(c ⇒ Cassandra.session.execute(c))
    }
  }

  import eu.inn.binders.cassandra._
  def removeContent(documentUri: String) = cql"delete from content where document_uri=$documentUri".execute()
  def removeContent(documentUri: String, itemId: String) = cql"delete from content where document_uri=$documentUri and item_id=$itemId".execute()
}

object Cassandra extends CassandraCQLUnit(
  new ClassPathCQLDataSet("schema.cql", "hyper_storage_test"), null, 60000l
) {
  //this.startupTimeoutMillis = 60000l
  lazy val start = {
    before()
  }
}
