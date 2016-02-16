package akka.actor

object GuardianExtractor {
  def guardian(system: ActorSystem) = system.asInstanceOf[ActorSystemImpl].guardian
}
