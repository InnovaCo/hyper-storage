package eu.inn.revault

import java.util.{GregorianCalendar, Calendar, TimeZone, Date}
import java.util.zip.CRC32

import com.datastax.driver.core.utils.UUIDs
import eu.inn.revault.db.Monitor

object MonitorLogic {
  val MaxChannels: Int = 1024
  val timeZone = TimeZone.getTimeZone("UTC")

  def newMonitor(uri: String, revision: Long, body: String) = Monitor(
    dtQuantum = getDtQuantum(System.currentTimeMillis()),
    channel = channelFromUri(uri),
    uri = uri,
    revision = revision,
    uuid = UUIDs.timeBased(),
    body = body,
    completedAt = None
  )

  def channelFromUri(uri: String): Int = {
    val crc = new CRC32()
    crc.update(uri.getBytes("UTF-8"))
    (crc.getValue % MaxChannels).toInt
  }

  def getDtQuantum(unixTime: Long): Long = {
    unixTime / (1000 * 60)
  }
}
