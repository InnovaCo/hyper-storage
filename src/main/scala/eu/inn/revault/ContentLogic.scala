package eu.inn.revault

import eu.inn.hyperbus.transport.api.uri.{UriParser, Uri}
import eu.inn.revault.db.Content

object ContentLogic {
  implicit class ContentWrapper(val content: Content) {
    def uri = {
      if (content.itemSegment.isEmpty)
        content.documentUri
      else
        content.documentUri + "/" + content.itemSegment
    }
    def partition = TransactionLogic.partitionFromUri(uri)
  }

  // todo: describe uri to resource/collection item matching
  def splitPath(path: String): (String,String) = {
    if (path.startsWith("/") || path.endsWith("/"))
      throw new IllegalArgumentException(s"$path is invalid (ends or starts with '/')")

    if (path.exists { char ⇒
      !(
        Character.isLetterOrDigit(char) ||
          char == '/' ||
          char == '-' ||
          char == '_' ||
          char == '.' ||
          char == '='
        )
    }) throw new IllegalArgumentException(s"$path contains invalid characters")

    val segments = path.split('/')
    if (segments.isEmpty || segments.exists(_.isEmpty))
      throw new IllegalArgumentException(s"$path is invalid (empty segments)")

    if (segments.length % 2 == 0) {
      // collection item
      val r = segments.reverse
      val documentUri = r.tail.reverse.mkString("/")
      val itemSegment = r.head
      (documentUri, itemSegment)
    } else {
      // document
      (path, "")
    }
  }
}
