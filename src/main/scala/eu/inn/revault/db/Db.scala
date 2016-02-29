package eu.inn.revault.db

import java.util.{Date, UUID}

import eu.inn.binders._
import eu.inn.binders.cassandra._
import eu.inn.binders.naming.CamelCaseToSnakeCaseConverter
import eu.inn.revault.CassandraConnector
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

case class Content(
                    documentUri: String,
                    itemSegment: String,
                    revision: Long,
                    monitorList: List[UUID],
                    body: Option[String],
                    isDeleted: Boolean,
                    createdAt: Date,
                    modifiedAt: Option[Date]
                  )

case class Monitor(
                    dtQuantum: Long,
                    channel: Int,
                    uri: String,
                    uuid: UUID,
                    revision: Long,
                    body: String,
                    completedAt: Option[Date]
                  )

private [db] case class CheckPoint(lastQuantum: Long)

class Db(connector: CassandraConnector)(implicit ec: ExecutionContext) {
  private [this] lazy val session: com.datastax.driver.core.Session = connector.connect()
  private [this] lazy implicit val sessionQueryCache = new SessionQueryCache[CamelCaseToSnakeCaseConverter](session)
  val log = LoggerFactory.getLogger(getClass)

  def preStart(): Unit = {
    session
  }

  def close() = {
    try {
      val cluster = session.getCluster
      session.close()
      cluster.close()
    }
    catch {
      case NonFatal(e) ⇒
        log.error(s"Can't close C* session", e)
    }
  }

  def selectContent(documentUri: String, itemSegment: String): Future[Option[Content]] = cql"""
      select document_uri,item_segment,revision,monitor_list,body,is_deleted,created_at,modified_at from content
      where document_uri=$documentUri and item_segment=$itemSegment
    """.oneOption[Content]

  def insertContent(content: Content): Future[Unit] = cql"""
      insert into content(document_uri,item_segment,revision,monitor_list,body,is_deleted,created_at,modified_at)
      values(?,?,?,?,?,?,?,?)
    """.bind(content).execute()

  def selectMonitor(dtQuantum: Long, channel: Int, uri: String, uuid: UUID): Future[Option[Monitor]] = cql"""
      select dt_quantum,channel,uri,uuid,revision,body,completed_at from monitor
      where dt_quantum=$dtQuantum and channel=$channel and uri=$uri and uuid=$uuid
    """.oneOption[Monitor]

  def selectChannelMonitors(dtQuantum: Long, channel: Int): Future[Iterator[Monitor]] = cql"""
      select dt_quantum,channel,uri,uuid,revision,body,completed_at from monitor
      where dt_quantum=$dtQuantum and channel=$channel
    """.all[Monitor]

  def insertMonitor(monitor: Monitor): Future[Unit] = cql"""
      insert into monitor(dt_quantum,channel,uri,uuid,revision,body,completed_at)
      values(?,?,?,?,?,?,?)
    """.bind(monitor).execute()

  def completeMonitor(monitor: Monitor): Future[Unit] = cql"""
      update monitor set completed_at=dateOf(now())
      where dt_quantum=${monitor.dtQuantum}
        and channel=${monitor.channel}
        and uri=${monitor.uri}
        and uuid=${monitor.uuid}
    """.execute()

  def selectCheckpoint(channel: Int): Future[Option[Long]] = cql"""
      select last_quantum from checkpoint where channel = $channel
    """.oneOption[CheckPoint].map(_.map(_.lastQuantum))

  def updateCheckpoint(channel: Int, lastQuantum: Long): Future[Unit] = cql"""
      insert into checkpoint(channel, last_quantum) values($channel, $lastQuantum)
    """.execute()
}
