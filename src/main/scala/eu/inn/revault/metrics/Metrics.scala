package eu.inn.revault.metrics

object Metrics {
  val WORKER_PROCESS_TIME = "worker.process-time" // time of processing message by revault worker
  val COMPLETER_PROCESS_TIME = "completer.process-time" // time of processing message by revault completer
  val RETRIEVE_TIME = "retrieve-time" // time of retrieving data (get request)

  val SHARD_PROCESSOR_STASH_METER = "shard-stash-meter"
  val SHARD_PROCESSOR_TASK_METER = "shard-message-meter"
  val SHARD_PROCESSOR_FORWARD_METER = "shard-forward-meter"

  val HOT_QUANTUM_TIMER = "recovery.hot-quantum-timer"
  val HOT_INCOMPLETE_METER = "recovery.hot-incomplete-meter"

  val STALE_QUANTUM_TIMER = "recovery.stale-quantum-timer"
  val STALE_INCOMPLETE_METER = "recovery.stale-incomplete-meter"
}
