package eu.inn.hyperstorage

import eu.inn.hyperstorage.db.{Content, ContentStatic}

case class ResourcePath(documentUri: String, itemId: String)

object ContentLogic {

  // ? & # is not allowed, it means that query have been sent
  val allowedCharSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~:/[]@!$&'()*+,;=".toSet

  // todo: describe uri to resource/collection item matching
  def splitPath(path: String): ResourcePath = {
    if (path.startsWith("/") || path.endsWith("/"))
      throw new IllegalArgumentException(s"$path is invalid (ends or starts with '/')")

    if (path.exists(c ⇒ !allowedCharSet.contains(c)))
      throw new IllegalArgumentException(s"$path contains invalid characters")

    val segments = path.split('/')
    if (segments.isEmpty || segments.exists(_.isEmpty))
      throw new IllegalArgumentException(s"$path is invalid (empty segments)")

    if (segments.length > 1) {
      // collection item
      val r = segments.reverse
      val a = r(1)
      if (isCollectionUri(a)) {
        val documentUri = r.tail.reverse.mkString("/")
        val itemId = r.head
        ResourcePath(documentUri, itemId)
      }
      else {
        ResourcePath(path, "")
      }
    } else {
      // document
      ResourcePath(path, "")
    }
  }

  def isCollectionUri(path: String): Boolean = path.endsWith("~")

  implicit class ContentWrapper(val content: Content) {
    def uri = {
      if (content.itemId.isEmpty)
        content.documentUri
      else
        content.documentUri + "/" + content.itemId
    }

    def partition = TransactionLogic.partitionFromUri(content.documentUri)
  }

  implicit class ContentStaticWrapper(val content: ContentStatic) {
    def partition = TransactionLogic.partitionFromUri(content.documentUri)
  }
}
