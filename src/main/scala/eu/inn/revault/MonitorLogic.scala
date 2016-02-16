package eu.inn.revault

import java.util.{GregorianCalendar, Calendar, TimeZone, Date}
import java.util.zip.CRC32

import eu.inn.revault.db.Monitor

object MonitorLogic {
  val MaxChannels: Int = 1024
  val timeZone = TimeZone.getTimeZone("UTC")

  def newMonitor(path: String, revision: Long, body: String) = Monitor(
    dt = roundDate,
    channel = channelFromPath(path),
    uri = path,
    revision = revision,
    body = body,
    completedAt = None
  )

  def channelFromPath(path: String): Int = {
    val crc = new CRC32()
    crc.update(path.getBytes("UTF-8"))
    (crc.getValue % MaxChannels).toInt
  }

  def roundDate: Date = {
    val calendar = new GregorianCalendar(timeZone)
    calendar.set(Calendar.SECOND, 0)
    calendar.set(Calendar.MILLISECOND, 0)
    calendar.getTime
  }
}
