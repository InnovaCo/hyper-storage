package eu.inn.hyperstorage.utils

import scala.collection.generic.CanBuildFrom
import scala.collection.{Seq, mutable}
import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by maqdev on 02.03.16.
  */
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
