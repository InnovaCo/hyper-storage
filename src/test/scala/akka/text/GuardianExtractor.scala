package akka.text

import akka.actor.{ActorSystem, ActorSystemImpl}

object GuardianExtractor {
  def guardian(system: ActorSystem) = system.asInstanceOf[ActorSystemImpl].guardian
}
