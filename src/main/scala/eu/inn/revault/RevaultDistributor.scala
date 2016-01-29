package eu.inn.revault

import akka.actor.{Actor, ActorRef}
import eu.inn.binders.dynamic.Text
import eu.inn.hyperbus.HyperBus
import eu.inn.hyperbus.akkaservice.AkkaHyperService
import eu.inn.hyperbus.model.DynamicBody
import eu.inn.hyperbus.model.standard.{EmptyBody, NoContent, Ok}
import eu.inn.revault.protocol.{RevaultPut, RevaultGet}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global // todo: inject

class RevaultDistributor(revaultProcessor: ActorRef) extends Actor {

  def receive = AkkaHyperService.dispatch(this)

  def ~> (implicit request: RevaultGet) = {
      Future {
        val result = DynamicBody(Text("Yey"))
        Ok(result)
      }
    }

  def ~> (implicit request: RevaultPut) = {
    Future {
      NoContent(EmptyBody)
    }
  }
}
