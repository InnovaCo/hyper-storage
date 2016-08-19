import com.datastax.driver.core.Session
import eu.inn.hyperstorage.CassandraConnector
import eu.inn.hyperstorage.db.Db
import org.cassandraunit.CassandraCQLUnit
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

trait CassandraFixture extends BeforeAndAfterAll with ScalaFutures {
  this: Suite =>
  private[this] val log = LoggerFactory.getLogger(getClass)
  var session: Session = null
  var db: Db = null

  implicit def executionContext: ExecutionContext

  override def beforeAll() {
    Cassandra.start
    val connector = new CassandraConnector {
      override def connect(): Session = Cassandra.session
    }
    db = new Db(connector)
  }

  override def afterAll() {
    session = null
    db = null
    //EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
  }

  def cleanUpCassandra(): Unit = {
    log.info("------- CLEANING UP C* -------- ")
    import scala.collection.JavaConversions._
    val cleanDs = new ClassPathCQLDataSet("cleanup.cql", "hyper_storage_test")
    cleanDs.getCQLStatements.foreach(c ⇒ Cassandra.session.execute(c))
  }
}

object Cassandra extends CassandraCQLUnit(
  new ClassPathCQLDataSet("schema.cql", "hyper_storage_test")
) {
  this.startupTimeoutMillis = 60000l
  lazy val start = {
    before()
  }
}
