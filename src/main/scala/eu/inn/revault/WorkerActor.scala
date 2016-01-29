package eu.inn.revault

import akka.actor.{ActorRef, Actor}
import eu.inn.binders.cassandra.SessionQueryCache
import eu.inn.binders.naming.SnakeCaseToCamelCaseConverter
import eu.inn.hyperbus.HyperBus
import eu.inn.revault.db.{Content, Monitor, Db}
import eu.inn.revault.protocol.RevaultPut

import scala.concurrent.Future

// todo: rename PutTask
// todo: rename client
@SerialVersionUID(1L) case class PutTask(key: String, ttl: Long, client: ActorRef, content: RevaultPut) extends Task {
  def isExpired = ttl < System.currentTimeMillis()
}

// todo: rename WorkerActor
class WorkerActor(hyperBus: HyperBus, db: Db) extends Actor {
  def receive = {
    case put: PutTask ⇒
      putTask(put)
  }

  def putTask(put: PutTask): Unit = {
    context.become(putTaskWaitResult)
    val (prefix,lastSegment) = splitPath(put.content.path)

    // fetch and complete existing content
    val futureExistingContent: Future[Option[Content]] = db.selectContent(prefix,lastSegment) flatMap {
      case None ⇒ Future(None)
      case Some(content) ⇒
        db.selectMonitor(content.monitorDt, content.monitorChannel, put.content.path) flatMap {
          case None ⇒ Future(Some(content)) // If no monitor, consider that it's complete
          case Some(monitor) ⇒
            if (monitor.isComplete)
              Future(Some(content))
            else
              Future(Some(content))//completePreviousTask(content,monitor)
        }
    }

    futureExistingContent

  }

  def completePreviousTask(content: Content, monitor: Monitor): Future[Content] = ??? // todo: implement

  def putTaskWaitResult: Receive = ???

  // todo: describe uri to resource/collection item matching
  def splitPath(path: String): (String,String) = {
    // todo: implement collections
    (path,"")
  }
}

/*

1. Check existing resource monitor
2. complete if previous update is not complete
3. create & insert new monitor
4. update resource
5. send accepted to the client (if any)
6. publish event
7. when event is published complete monitor
8. request next task

*/
