package eu.inn.revault.protocol

import java.util.Date

import eu.inn.binders.annotations.fieldName
import eu.inn.hyperbus.model._
import eu.inn.hyperbus.model.annotations.{body, request}

@request(Method.GET, "/revault/{path:*}") // todo: check if raml arg can be with pattern
case class RevaultGet(path: String, body: EmptyBody) extends Request[EmptyBody]
with DefinedResponse[Ok[DynamicBody]]

@body("revault-monitor")
case class Monitor(id: String, status: String, completedAt: Option[Date],
                   @fieldName("_links") links: LinksMap.LinksMapType = LinksMap("/revault-monitors/{id}")) extends Body with Links

@request(Method.PUT, "/revault/{path:*}")
case class RevaultPut(path: String, body: DynamicBody) extends Request[DynamicBody]
with DefinedResponse[(
    //Created[DynamicBody with CreatedBody],
    Accepted[Monitor],
    NoContent[EmptyBody]
  )]

@request(Method.FEED_PUT, "/revault/{path:*}")
case class RevaultFeedPut(path: String, body: DynamicBody) extends Request[DynamicBody]

@request(Method.PATCH, "/revault/{path:*}")
case class RevaultPatch(path: String, body: DynamicBody) extends Request[DynamicBody]
  with DefinedResponse[(
      Accepted[Monitor],
      NoContent[EmptyBody]
    )]

@request(Method.FEED_PATCH, "/revault/{path:*}")
case class RevaultFeedPatch(path: String, body: DynamicBody) extends Request[DynamicBody]

@request(Method.DELETE, "/revault/{path:*}")
case class RevaultDelete(path: String, body: EmptyBody) extends Request[EmptyBody]
  with DefinedResponse[(
      Accepted[Monitor],
      NoContent[EmptyBody]
    )]

@request(Method.FEED_DELETE, "/revault/{path:*}")
case class RevaultFeedDelete(path: String, body: EmptyBody) extends Request[EmptyBody]

/*

*/
// collection is specified by _links.parent -> ... parent
