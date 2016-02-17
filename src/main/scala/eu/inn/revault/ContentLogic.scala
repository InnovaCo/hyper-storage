package eu.inn.revault

import eu.inn.revault.db.Content

object ContentLogic {
  implicit class ContentWrapper(val content: Content) {
    def uri = {
      if (content.itemSegment.isEmpty)
        content.documentUri
      else
        content.documentUri + "/" + content.itemSegment
    }
    def monitorChannel = MonitorLogic.channelFromUri(uri)
  }

  // todo: describe uri to resource/collection item matching
  def splitPath(path: String): (String,String) = {
    // todo: implement collections
    (path,"")
  }
}
