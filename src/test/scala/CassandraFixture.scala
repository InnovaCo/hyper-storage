import com.datastax.driver.core.Session
import eu.inn.revault.db.Db
import org.cassandraunit.CassandraCQLUnit
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, Suite}
import scala.concurrent.ExecutionContext.Implicits.global

object Cassandra extends CassandraCQLUnit(
    new ClassPathCQLDataSet("schema.cql","revault_test"), null, "127.0.0.1", 9142, 20000
  ) {
  lazy val start = {
    before()
  }
}

trait CassandraFixture extends BeforeAndAfterAll {
  this: Suite =>
  var session: Session = null
  var db: Db = null

  override def beforeAll() {
    Cassandra.start
    session = Cassandra.session
    db = new Db(session)
  }

  override def afterAll() {
    session = null
    db = null
    //EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
  }
}
