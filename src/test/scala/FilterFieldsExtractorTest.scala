import eu.inn.hyperstorage.api.{HyperStorageIndexSortItem, HyperStorageIndexSortOrder}
import eu.inn.parser.HParser
import org.scalatest.{FreeSpec, Matchers}

object FilterFieldsExtractor {

}

class FilterFieldsExtractorTest extends FreeSpec with Matchers {
  "FilterFieldsExtractor" - {
    "Single gt filter field" in {
      val expression = HParser(s""" id > "10" """).get
      val sortByFields =  Seq(HyperStorageIndexSortItem("id", None, Some(HyperStorageIndexSortOrder.ASC)))
    }
  }
}
