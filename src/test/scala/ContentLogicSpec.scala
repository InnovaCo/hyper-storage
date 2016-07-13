import eu.inn.hyperstorage.{ContentLogic, ResourcePath}
import org.scalatest.{FreeSpec, Matchers}

class ContentLogicSpec extends FreeSpec with Matchers{
  "ContentLogic" - {
    "splitPath should parse document" in {
      ContentLogic.splitPath("document") should equal(ResourcePath("document", ""))
      ContentLogic.splitPath("some/other/document") should equal(ResourcePath("some/other/document", ""))
    }

    "splitPath should parse collection item" in {
      ContentLogic.splitPath("document~/item") should equal(ResourcePath("document~", "item"))
      ContentLogic.splitPath("some/other/document~/item") should equal(ResourcePath("some/other/document~", "item"))
    }

    "splitPath should fail when invalid chars are used" in {
      intercept [IllegalArgumentException] {
        ContentLogic.splitPath(" ")
      }
      intercept [IllegalArgumentException] {
        ContentLogic.splitPath("\t")
      }
    }

    "splitPath should fail when invalid URI is used" in {
      intercept [IllegalArgumentException] {
        ContentLogic.splitPath("/")
      }
      intercept [IllegalArgumentException] {
        ContentLogic.splitPath("//")
      }
      intercept [IllegalArgumentException] {
        ContentLogic.splitPath("/a")
      }
      intercept [IllegalArgumentException] {
        ContentLogic.splitPath("a/")
      }
      intercept [IllegalArgumentException] {
        ContentLogic.splitPath("a//b")
      }
    }
  }
}
