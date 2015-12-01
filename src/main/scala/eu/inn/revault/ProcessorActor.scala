package eu.inn.revault

import akka.actor.Actor
import akka.cluster.Cluster
import akka.routing.ConsistentHash

case class ShardMessage(key: String, content: Any)

class ProcessorActor(cluster: Cluster) extends Actor {

  val c = ConsistentHash(Seq[Int](), 100)

  //c.nodeFor()

  def resume = ???
  def pause = ???



  def receive = {
    case _ ⇒
  }
}

/*

1. start

  1.1. status = paused
  1.2. wait while cluster is up
  1.3. create hash ring
  1.4. start notifying each member that i'm ready to resume
  1.5. if got message from everyone, resume

2. shutdown (status = processing)
  2.1. status = pausing
  2.2. for each member:
    2.2.1. check that started operations that has to be owned by that member is complete
    2.2.2. notify a member about new ownership
  2.3. send poison kill to self
  2.4. forward all

*/
