package eu.inn.revault.metrics

object Metrics {
  val WORKER_PROCESS_TIME = "revault.worker.process-time" // time of processing message by revault worker
  val COMPLETER_PROCESS_TIME = "revault.completer.process-time" // time of processing message by revault completer
  val RETRIEVE_TIME = "revault.retrieve-time" // time of retrieving data (get request)

  val SHARD_PROCESSOR_STASH_METER = "revault.shard-stash-meter"
  val SHARD_PROCESSOR_TASK_METER = "revault.shard-message-meter"
  val SHARD_PROCESSOR_FORWARD_METER = "revault.shard-forward-meter"

  val HOT_QUANTUM_TIMER = "revault.recovery.hot-quantum-timer"
  val HOT_INCOMPLETE_METER = "revault.recovery.hot-incomplete-meter"

  val STALE_QUANTUM_TIMER = "revault.recovery.stale-quantum-timer"
  val STALE_INCOMPLETE_METER = "revault.recovery.stale-incomplete-meter"
}
