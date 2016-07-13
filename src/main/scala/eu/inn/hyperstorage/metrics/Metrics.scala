package eu.inn.hyperstorage.metrics

object Metrics {
  val WORKER_PROCESS_TIME = "hyper-storage.worker.process-time" // time of processing message by hyper-storage worker
  val COMPLETER_PROCESS_TIME = "hyper-storage.completer.process-time" // time of processing message by hyper-storage completer
  val RETRIEVE_TIME = "hyper-storage.retrieve-time" // time of retrieving data (get request)

  val SHARD_PROCESSOR_STASH_METER = "hyper-storage.shard-stash-meter"
  val SHARD_PROCESSOR_TASK_METER = "hyper-storage.shard-message-meter"
  val SHARD_PROCESSOR_FORWARD_METER = "hyper-storage.shard-forward-meter"

  val HOT_QUANTUM_TIMER = "hyper-storage.recovery.hot-quantum-timer"
  val HOT_INCOMPLETE_METER = "hyper-storage.recovery.hot-incomplete-meter"

  val STALE_QUANTUM_TIMER = "hyper-storage.recovery.stale-quantum-timer"
  val STALE_INCOMPLETE_METER = "hyper-storage.recovery.stale-incomplete-meter"
}
