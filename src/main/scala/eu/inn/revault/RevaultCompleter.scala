package eu.inn.revault

import java.util.UUID

import akka.actor.{Status, Actor, ActorLogging, ActorRef}
import akka.pattern.pipe
import com.datastax.driver.core.utils.UUIDs
import eu.inn.hyperbus.Hyperbus
import eu.inn.hyperbus.model.DynamicRequest
import eu.inn.revault.db.{ContentStatic, Db, Transaction}
import eu.inn.revault.sharding.{ShardTask, ShardTaskComplete}
import eu.inn.revault.utils.FutureUtils

import scala.collection.Seq
import scala.concurrent.Future
import scala.util.Success
import scala.util.control.NonFatal

@SerialVersionUID(1L) case class RevaultCompleterTask(ttl: Long, documentUri: String) extends ShardTask {
  def key = documentUri
  def isExpired = ttl < System.currentTimeMillis()
  def group = "revault-completer"
}

@SerialVersionUID(1L) case class RevaultCompleterTaskResult(documentUri: String, transactions: Seq[UUID])
@SerialVersionUID(1L) case class NoSuchResourceException(documentUri: String) extends RuntimeException(s"No such resource: $documentUri")
@SerialVersionUID(1L) case class IncorrectDataException(documentUri: String, reason: String) extends RuntimeException(s"Data for $documentUri is incorrect: $reason")
@SerialVersionUID(1L) case class CompletionFailedException(documentUri: String, reason: String) extends RuntimeException(s"Complete for $documentUri is failed: $reason")

// todo: rename this
class RevaultCompleter(hyperbus: Hyperbus, db: Db) extends Actor with ActorLogging {
  import ContentLogic._
  import context._

  override def receive: Receive = {
    case task: RevaultCompleterTask ⇒
      executeTask(sender(), task)
  }

  def executeTask(owner: ActorRef, task: RevaultCompleterTask): Unit = {
    val (documentUri, itemSegment) = ContentLogic.splitPath(task.documentUri)
    if (!itemSegment.isEmpty) {
      owner ! Status.Success { // todo: is this have to be a success
        val e = new IllegalArgumentException(s"RevaultCompleter task key ${task.key} doesn't correspond to $documentUri")
        ShardTaskComplete(task, e)
      }
    }
    else {
      db.selectContentStatic(task.documentUri) flatMap {
        case None ⇒
          log.error(s"Didn't found resource to complete, dismissing task: $task")
          Future(ShardTaskComplete(task, new NoSuchResourceException(task.documentUri)))
        case Some(content) ⇒
          try {
            completeTransactions(task, content)
          } catch {
            case NonFatal(e) ⇒
              log.error(e, s"Task $task didn't complete")
              Future(ShardTaskComplete(task, e))
          }
      } pipeTo owner
    }
  }

  def completeTransactions(task: RevaultCompleterTask, content: ContentStatic): Future[ShardTaskComplete] = {
    if (content.transactionList.isEmpty) {
      throw new IncorrectDataException(content.documentUri, "empty transaction list")
    }
    else {
      selectIncompleteTransactions(content) flatMap { incompleteTransactions ⇒
        FutureUtils.serial(incompleteTransactions) { transaction ⇒
          val event = DynamicRequest(transaction.body)
          hyperbus <| event flatMap { publishResult ⇒
            if (log.isDebugEnabled) {
              log.debug(s"Event $event is published with result $publishResult")
            }
            db.completeTransaction(transaction) map { _ ⇒
              if (log.isDebugEnabled) {
                log.debug(s"$transaction is complete")
              }
              transaction
            }
          }
        } map { updatedTransactions ⇒
          ShardTaskComplete(task, RevaultCompleterTaskResult(task.documentUri, updatedTransactions.map(_.uuid)))
        } recover {
          case NonFatal(e) ⇒
            ShardTaskComplete(task, CompletionFailedException(task.documentUri, e.toString))
        } andThen {
          case Success(ShardTaskComplete(_, RevaultCompleterTaskResult(documentUri, updatedTransactions))) ⇒
            log.debug(s"Removing completed transactions $updatedTransactions from $documentUri")
            db.removeCompleteTransactionsFromList(documentUri, updatedTransactions.toList) recover {
              case NonFatal(e) ⇒
                log.error(e, s"Can't remove complete transactions $updatedTransactions from $documentUri")
            }
        }
      }
    }
  }

  def selectIncompleteTransactions(content: ContentStatic): Future[Seq[Transaction]] = {
    val transactionsFStream = content.transactionList.toStream.map { transactionUuid ⇒
      val quantum = TransactionLogic.getDtQuantum(UUIDs.unixTimestamp(transactionUuid))
      db.selectTransaction(quantum, content.partition, content.documentUri, transactionUuid)
    }
    FutureUtils.collectWhile(transactionsFStream) {
      case Some(transaction) ⇒ transaction
    } map (_.reverse)
  }
}


