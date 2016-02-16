package eu.inn.revault.db

import java.util.{UUID, Date}

import com.typesafe.config.Config
import eu.inn.binders._
import eu.inn.binders.cassandra._
import eu.inn.binders.naming.{CamelCaseToSnakeCaseConverter, SnakeCaseToCamelCaseConverter}

import scala.concurrent.{Future, ExecutionContext}

case class Content(
                    documentUri: String,
                    itemSegment: String,
                    revision: Long,
                    monitorDtQuantum: Date,
                    monitorChannel: Int,
                    monitorUuid: UUID,
                    body: Option[String],
                    isDeleted: Boolean,
                    createdAt: Date,
                    modifiedAt: Option[Date]
                  )

case class Monitor(
                    dtQuantum: Date,
                    channel: Int,
                    uri: String,
                    revision: Long,
                    uuid: UUID,
                    body: String,
                    completedAt: Option[Date]
                  )

case class Channel(
                    channel: Int,
                    lastQuantum: Date
                  )

class Db(session: com.datastax.driver.core.Session)(implicit ec: ExecutionContext) {
  private [this] implicit val sessionQueryCache = new SessionQueryCache[CamelCaseToSnakeCaseConverter](session)

  def selectContent(documentUri: String, itemSegment: String): Future[Option[Content]] = cql"""
      select document_uri,item_segment,revision,monitor_dt_quantum,monitor_channel,monitor_uuid,body,is_deleted,created_at,modified_at from content
      where document_uri=$documentUri and item_segment=$itemSegment
    """.oneOption[Content]

  def insertContent(content: Content): Future[Unit] = cql"""
      insert into content(document_uri,item_segment,revision,monitor_dt_quantum,monitor_channel,monitor_uuid,body,is_deleted,created_at,modified_at)
      values(?,?,?,?,?,?,?,?,?,?)
    """.bind(content).execute()

  def selectMonitor(dtQuantum: Date, channel: Int, uri: String, revision: Long, uuid: UUID): Future[Option[Monitor]] = cql"""
      select dt_quantum,channel,uri,revision,uuid,body,completed_at from monitor
      where dt_quantum=$dtQuantum and channel=$channel and uri=$uri and revision = $revision and uuid=$uuid
    """.oneOption[Monitor]

  def insertMonitor(monitor: Monitor): Future[Unit] = cql"""
      insert into monitor(dt_quantum,channel,uri,revision,uuid,body,completed_at)
      values(?,?,?,?,?,?,?)
    """.bind(monitor).execute()

  def completeMonitor(monitor: Monitor): Future[Unit] = cql"""
      update monitor set completed_at=dateOf(now())
      where dt_quantum=${monitor.dtQuantum}
        and channel=${monitor.channel}
        and uri=${monitor.uri}
        and revision=${monitor.revision}
        and uuid=${monitor.uuid}
    """.execute()
}
