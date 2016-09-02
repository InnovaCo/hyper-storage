package eu.inn.hyperstorage.utils

/*

this seems much slower than sorted.take(n)!

object SortUtils {
  // credit goes to http://stackoverflow.com/questions/5674741/simplest-way-to-get-the-top-n-elements-of-a-scala-iterable
  implicit def iterExt[A](iter: Iterable[A]) = new {
    def sortedTop[B](n: Int, f: A => B)(implicit ord: Ordering[B]): List[A] = {
      def updateSofar (sofar: List [A], el: A): List [A] = {
        if (ord.compare(f(el), f(sofar.head)) > 0)
          (el +: sofar.tail).sortBy (f)
        else sofar
      }
      val (sofar, rest) = iter.splitAt(n)
      (sofar.toList.sortBy (f) /: rest) (updateSofar).reverse
    }
  }
}

case class TestSortObject(key: String, data: String)

object TopSortBenchmark extends Bench.LocalTime {
  val seeds = Gen.range("seed/size-k")(4, 10, 2)
  val stringStreams = seeds.map { seed ⇒
    val rand = new Random(seed)
    val size = seed * 500
    val stream = (0 to size).map(_ ⇒
      TestSortObject(rand.nextString(10+seed*2),rand.nextString(10+seed*4))
    ).toStream
    (stream, size)
  }

  performance of "TopSort" in {
    measure method "sortedTop" in {
      using(stringStreams) in { case (stream, size) ⇒
        import SortUtils._
        stream.sortedTop(size/3, v⇒v.key)
      }
    }

    measure method "sorted/take" in {
      using(stringStreams) in { case (stream, size) ⇒
        stream.sortBy(_.key).take(size/3)
      }
    }
  }
}


*/
