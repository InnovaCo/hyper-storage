package eu.inn.hyperstorage.metrics

object Metrics {
  // time of processing message by primary worker
  val PRIMARY_PROCESS_TIME = "hyper-storage.primary.process-time"

  // time of processing message by secondary worker
  val SECONDARY_PROCESS_TIME = "hyper-storage.secondary.process-time"

  // time of retrieving data (get request)
  val RETRIEVE_TIME = "hyper-storage.retrieve-time"

  val SHARD_PROCESSOR_STASH_METER = "hyper-storage.shard-stash-meter"
  val SHARD_PROCESSOR_TASK_METER = "hyper-storage.shard-message-meter"
  val SHARD_PROCESSOR_FORWARD_METER = "hyper-storage.shard-forward-meter"

  val HOT_QUANTUM_TIMER = "hyper-storage.recovery.hot-quantum-timer"
  val HOT_INCOMPLETE_METER = "hyper-storage.recovery.hot-incomplete-meter"

  val STALE_QUANTUM_TIMER = "hyper-storage.recovery.stale-quantum-timer"
  val STALE_INCOMPLETE_METER = "hyper-storage.recovery.stale-incomplete-meter"
}
