package eu.inn.hyperstorage.utils

import java.util.concurrent.atomic.AtomicLong

object AkkaNaming {
  private val id = new AtomicLong()
  def next(prefix: String): String = {
    prefix + id.incrementAndGet().toHexString
  }
}
