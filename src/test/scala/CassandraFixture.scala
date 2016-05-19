import com.datastax.driver.core.Session
import eu.inn.revault.CassandraConnector
import eu.inn.revault.db.Db
import org.cassandraunit.CassandraCQLUnit
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.ExecutionContext.Implicits.global

object Cassandra extends CassandraCQLUnit(
    new ClassPathCQLDataSet("schema.cql","revault_test")
  ) {
  this.startupTimeoutMillis = 30000l
  lazy val start = {
    before()
  }
}

trait CassandraFixture extends BeforeAndAfterAll with ScalaFutures {
  this: Suite =>
  var session: Session = null
  var db: Db = null

  implicit def executionContext: ExecutionContext

  private [this] val log = LoggerFactory.getLogger(getClass)

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
    val cleanDs = new ClassPathCQLDataSet("cleanup.cql", "revault_test")
    cleanDs.getCQLStatements.foreach(c ⇒ Cassandra.session.execute(c))
  }
}
