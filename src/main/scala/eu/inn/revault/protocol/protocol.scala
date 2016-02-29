package eu.inn.revault.protocol

import java.util.Date

import eu.inn.binders.annotations.fieldName
import eu.inn.hyperbus.model._
import eu.inn.hyperbus.model.annotations.{body, request}

@request(Method.GET, "/revault/content/{path:*}") // todo: check if raml arg can be with pattern
case class RevaultGet(path: String, body: EmptyBody) extends Request[EmptyBody]
with DefinedResponse[Ok[DynamicBody]]

@body("revault-transaction")
case class Transaction(id: String, status: String, completedAt: Option[Date],
                       @fieldName("_links") links: LinksMap.LinksMapType = LinksMap("/revault-transaction/{id}")) extends Body with Links

@request(Method.PUT, "/revault/content/{path:*}")
case class RevaultPut(path: String, body: DynamicBody) extends Request[DynamicBody]
with DefinedResponse[(
    //Created[DynamicBody with CreatedBody],
    Accepted[Transaction],
    NoContent[EmptyBody]
  )]

@request(Method.FEED_PUT, "/revault/content/{path:*}")
case class RevaultFeedPut(path: String, body: DynamicBody) extends Request[DynamicBody]

@request(Method.PATCH, "/revault/content/{path:*}")
case class RevaultPatch(path: String, body: DynamicBody) extends Request[DynamicBody]
  with DefinedResponse[(
      Accepted[Transaction],
      NoContent[EmptyBody]
    )]

@request(Method.FEED_PATCH, "/revault/content/{path:*}")
case class RevaultFeedPatch(path: String, body: DynamicBody) extends Request[DynamicBody]

@request(Method.DELETE, "/revault/content/{path:*}")
case class RevaultDelete(path: String, body: EmptyBody) extends Request[EmptyBody]
  with DefinedResponse[(
      Accepted[Transaction],
      NoContent[EmptyBody]
    )]

@request(Method.FEED_DELETE, "/revault/content/{path:*}")
case class RevaultFeedDelete(path: String, body: EmptyBody) extends Request[EmptyBody]

/*

*/
// collection is specified by _links.parent -> ... parent
