package eu.inn.revault.protocol

import java.util.Date

import eu.inn.binders.annotations.fieldName
import eu.inn.hyperbus.model._
import eu.inn.hyperbus.model.annotations.{body, request}

// todo: add monitor body!

@request(Method.GET, "/revault/content/{path:*}") // todo: check if raml arg can be with pattern
case class RevaultGet(path: String, body: Query) extends Request[Query]
with DefinedResponse[Ok[DynamicBody]]

@body("revault-transaction")
case class Transaction(transactionId: String,
                       @fieldName("_links") links: LinksMap.LinksMapType = LinksMap("/revault-transaction/{transactionId}")) extends Body with Links

@body("revault-transaction-created")
case class TransactionCreated(transactionId: String,
                       @fieldName("_links") links: LinksMap.LinksMapType = LinksMap("/revault-transaction/{transactionId}")) extends Body with Links with CreatedBody

@request(Method.PUT, "/revault/content/{path:*}")
case class RevaultPut(path: String, body: DynamicBody) extends Request[DynamicBody]
with DefinedResponse[(
    Created[TransactionCreated],
    Ok[DynamicBody]
  )]

@request(Method.FEED_PUT, "/revault/content/{path:*}")
case class RevaultFeedPut(path: String, body: DynamicBody) extends Request[DynamicBody]

@request(Method.PATCH, "/revault/content/{path:*}")
case class RevaultPatch(path: String, body: DynamicBody) extends Request[DynamicBody]
  with DefinedResponse[Ok[DynamicBody]]

@request(Method.FEED_PATCH, "/revault/content/{path:*}")
case class RevaultFeedPatch(path: String, body: DynamicBody) extends Request[DynamicBody]

@request(Method.DELETE, "/revault/content/{path:*}")
case class RevaultDelete(path: String, body: EmptyBody) extends Request[EmptyBody]
  with DefinedResponse[Ok[DynamicBody]]

@request(Method.FEED_DELETE, "/revault/content/{path:*}")
case class RevaultFeedDelete(path: String, body: EmptyBody) extends Request[EmptyBody]

@request(Method.POST, "/revault/content/{path:*}")
case class RevaultPost(path: String, body: DynamicBody) extends Request[DynamicBody]
  with DefinedResponse[Created[TransactionCreated]]

//@request(Method.FEED_POST, "/revault/content/{path:*}")
//case class RevaultFeedPost(path: String, body: DynamicBody) extends Request[DynamicBody]
