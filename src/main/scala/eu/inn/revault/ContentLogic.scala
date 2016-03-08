package eu.inn.revault

import eu.inn.revault.db.{Content, ContentStatic}

object ContentLogic {
  implicit class ContentWrapper(val content: Content) {
    def uri = {
      if (content.itemSegment.isEmpty)
        content.documentUri
      else
        content.documentUri + "/" + content.itemSegment
    }
    def partition = TransactionLogic.partitionFromUri(content.documentUri)
  }

  implicit class ContentStaticWrapper(val content: ContentStatic) {
    def partition = TransactionLogic.partitionFromUri(content.documentUri)
  }

  // ? & # is not allowed, it means that query have been sent
  val allowedCharSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~:/[]@!$&'()*+,;=".toSet
  // todo: describe uri to resource/collection item matching
  def splitPath(path: String): (String,String) = {
    if (path.startsWith("/") || path.endsWith("/"))
      throw new IllegalArgumentException(s"$path is invalid (ends or starts with '/')")

    if (path.exists(c ⇒ !allowedCharSet.contains(c)))
      throw new IllegalArgumentException(s"$path contains invalid characters")

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
