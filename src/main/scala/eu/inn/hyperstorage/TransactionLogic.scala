package eu.inn.hyperstorage

import java.util.TimeZone
import java.util.zip.CRC32

import com.datastax.driver.core.utils.UUIDs
import eu.inn.hyperstorage.db.Transaction
import eu.inn.hyperstorage.sharding.{ShardTask, ShardedClusterData}

object TransactionLogic {
  final val MAX_TRANSACTIONS: Int = 1024
  final val timeZone = TimeZone.getTimeZone("UTC")

  def newTransaction(documentUri: String, itemId: String, revision: Long, body: String) = Transaction(
    dtQuantum = getDtQuantum(System.currentTimeMillis()),
    partition = partitionFromUri(documentUri),
    documentUri = documentUri,
    itemId = itemId,
    revision = revision,
    uuid = UUIDs.timeBased(),
    body = body,
    obsoleteIndexItems = None,
    completedAt = None
  )

  def partitionFromUri(uri: String): Int = {
    val crc = new CRC32()
    crc.update(uri.getBytes("UTF-8"))
    (crc.getValue % MAX_TRANSACTIONS).toInt
  }

  def getDtQuantum(unixTime: Long): Long = {
    unixTime / (1000 * 60)
  }

  def getUnixTimeFromQuantum(qt: Long): Long = {
    qt * 1000 * 60
  }

  def getPartitions(data: ShardedClusterData): Seq[Int] = {
    0 until TransactionLogic.MAX_TRANSACTIONS flatMap { partition ⇒
      val task = new ShardTask {
        def key = partition.toString

        def group = ""

        def isExpired = false
      }
      if (data.taskIsFor(task) == data.selfAddress)
        Some(partition)
      else
        None
    }
  }
}
