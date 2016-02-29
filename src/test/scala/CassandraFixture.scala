import com.datastax.driver.core.Session
import eu.inn.revault.CassandraConnector
import eu.inn.revault.db.Db
import org.cassandraunit.CassandraCQLUnit
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, Suite}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global

object Cassandra extends CassandraCQLUnit(
    new ClassPathCQLDataSet("schema.cql","revault_test"), null, 30000
  ) {
  lazy val start = {
    before()
  }
}

trait CassandraFixture extends BeforeAndAfterAll with ScalaFutures {
  this: Suite =>
  var session: Session = null
  var db: Db = null

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
}
