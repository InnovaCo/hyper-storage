package eu.inn.hyperstorage.utils

object SortUtils {
  // credit goes to http://stackoverflow.com/questions/5674741/simplest-way-to-get-the-top-n-elements-of-a-scala-iterable
  implicit def iterExt[A](iter: Iterable[A]) = new {
    def sortedTop[B](n: Int, f: A => B)(implicit ord: Ordering[B]): Stream[A] = {
      def updateSofar (sofar: Stream [A], el: A): Stream [A] = {
        if (ord.compare(f(el), f(sofar.head)) > 0)
          (el +: sofar.tail).sortBy (f)
        else sofar
      }
      val (sofar, rest) = iter.splitAt(n)
      (sofar.toStream.sortBy (f) /: rest) (updateSofar).reverse
    }
  }
}
