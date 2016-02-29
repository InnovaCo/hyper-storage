package eu.inn.revault

import java.util.TimeZone
import java.util.zip.CRC32

import com.datastax.driver.core.utils.UUIDs
import eu.inn.revault.db.Transaction

object TransactionLogic {
  val MaxPartitions: Int = 1024
  val timeZone = TimeZone.getTimeZone("UTC")

  def newTransaction(uri: String, revision: Long, body: String) = Transaction(
    dtQuantum = getDtQuantum(System.currentTimeMillis()),
    partition = partitionFromUri(uri),
    uri = uri,
    revision = revision,
    uuid = UUIDs.timeBased(),
    body = body,
    completedAt = None
  )

  def partitionFromUri(uri: String): Int = {
    val crc = new CRC32()
    crc.update(uri.getBytes("UTF-8"))
    (crc.getValue % MaxPartitions).toInt
  }

  def getDtQuantum(unixTime: Long): Long = {
    unixTime / (1000 * 60)
  }
}
