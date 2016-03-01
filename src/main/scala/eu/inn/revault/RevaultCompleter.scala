package eu.inn.revault

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.pattern.pipe
import com.datastax.driver.core.utils.UUIDs
import eu.inn.hyperbus.HyperBus
import eu.inn.hyperbus.model.DynamicRequest
import eu.inn.revault.db.{ContentStatic, Db, Transaction}
import eu.inn.revault.sharding.{ShardTask, ShardTaskComplete}

import scala.collection.generic.CanBuildFrom
import scala.collection.{Seq, mutable}
import scala.concurrent.{ExecutionContext, Future}
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

class RevaultCompleter(hyperBus: HyperBus, db: Db) extends Actor with ActorLogging {
  import ContentLogic._
  import context._

  override def receive: Receive = {
    case task: RevaultCompleterTask ⇒
      executeTask(sender(), task)
  }

  def executeTask(owner: ActorRef, task: RevaultCompleterTask): Unit = {
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

  def completeTransactions(task: RevaultCompleterTask, content: ContentStatic): Future[ShardTaskComplete] = {
    if (content.transactionList.isEmpty) {
      throw new IncorrectDataException(content.documentUri, "empty transaction list")
    }
    else {
      selectIncompleteTransactions(content) flatMap { incompleteTransactions ⇒
        FutureUtils.serial(incompleteTransactions) { transaction ⇒
          val event = DynamicRequest(transaction.body)
          hyperBus <| event flatMap { publishResult ⇒
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

object FutureUtils {
  /*def takeUntilNone[T](iterable: Iterable[Future[Option[T]]])(implicit ec: ExecutionContext): Future[Seq[T]] = {
    val iterator = iterable.iterator
    takeUntilNone(Seq.newBuilder[T],
      if (iterator.hasNext) iterator.next() else Future.successful(None)
    ) map {
      _.result()
    }
  }

  private def takeUntilNone[T](builder: mutable.Builder[T, Seq[T]], f: ⇒ Future[Option[T]])
                              (implicit ec: ExecutionContext): Future[mutable.Builder[T, Seq[T]]] = {
    f flatMap {
      case None ⇒ Future.successful(builder)
      case Some(t) ⇒ takeUntilNone(builder += t, f)
    }
  }*/

  def serial[A, B](in: Seq[A])(f: A ⇒ Future[B])(implicit ec: ExecutionContext): Future[Seq[B]] = {
    in.foldLeft(Future.successful(Seq.newBuilder[B])) { case (fr, a) ⇒
      for (result ← fr; r ← f(a)) yield result += r
    } map (_.result())
  }

  def collectWhile[A, B, M[X] <: Seq[X]](in: M[Future[A]])(pf: PartialFunction[A, B])(implicit cbf: CanBuildFrom[M[Future[A]], B, M[B]], ec: ExecutionContext): Future[M[B]] =
    collectWhileImpl(in, pf, cbf(in)).map(_.result())

  private def collectWhileImpl[A, B, M[X] <: Seq[X]](in: M[Future[A]], pf: PartialFunction[A, B], buffer: mutable.Builder[B, M[B]])(implicit ec: ExecutionContext): Future[mutable.Builder[B, M[B]]] =
    if (in.isEmpty) {
      Future.successful(buffer)
    } else {
      in.head flatMap {
        case r if pf.isDefinedAt(r) ⇒ collectWhileImpl(in.tail.asInstanceOf[M[Future[A]]], pf, buffer += pf(r))
        case _ ⇒ Future.successful(buffer)
      }
    }
}
