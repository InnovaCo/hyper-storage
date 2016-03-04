package eu.inn.revault.protocol

import java.util.Date

import eu.inn.binders.annotations.fieldName
import eu.inn.hyperbus.model._
import eu.inn.hyperbus.model.annotations.{body, request}

@request(Method.GET, "/revault/content/{path:*}") // todo: check if raml arg can be with pattern
case class RevaultGet(path: String, body: Query) extends Request[Query]
with DefinedResponse[Ok[DynamicBody]]

@body("revault-transaction")
case class Transaction(transactionId: String,
                       @fieldName("_links") links: Links.LinksMap = Transaction.defaultLinks) extends Body with Links

object Transaction {
  val selfPattern = "/revault-transaction/{transactionId}"
  val defaultLinks = Links(selfPattern, templated = true)
}

@body("revault-transaction-created")
case class TransactionCreated(transactionId: String, path: String,
                       @fieldName("_links") links: Links.LinksMap = TransactionCreated.defaultLinks) extends Body with Links with CreatedBody

object TransactionCreated {
  val defaultLinks = new LinksBuilder()
    .self(Transaction.selfPattern, templated = true)
    .location(RevaultGet.uriPattern)
    .result()
}

@request(Method.PUT, "/revault/content/{path:*}")
case class RevaultPut(path: String, body: DynamicBody) extends Request[DynamicBody]
with DefinedResponse[(
    Created[TransactionCreated],
    Ok[Transaction]
  )]

@request(Method.FEED_PUT, "/revault/content/{path:*}")
case class RevaultFeedPut(path: String, body: DynamicBody) extends Request[DynamicBody]

@request(Method.PATCH, "/revault/content/{path:*}")
case class RevaultPatch(path: String, body: DynamicBody) extends Request[DynamicBody]
  with DefinedResponse[Ok[Transaction]]

@request(Method.FEED_PATCH, "/revault/content/{path:*}")
case class RevaultFeedPatch(path: String, body: DynamicBody) extends Request[DynamicBody]

@request(Method.DELETE, "/revault/content/{path:*}")
case class RevaultDelete(path: String, body: EmptyBody) extends Request[EmptyBody]
  with DefinedResponse[Ok[Transaction]]

@request(Method.FEED_DELETE, "/revault/content/{path:*}")
case class RevaultFeedDelete(path: String, body: EmptyBody) extends Request[EmptyBody]

@request(Method.POST, "/revault/content/{path:*}")
case class RevaultPost(path: String, body: DynamicBody) extends Request[DynamicBody]
  with DefinedResponse[Created[TransactionCreated]]

//@request(Method.FEED_POST, "/revault/content/{path:*}")
//case class RevaultFeedPost(path: String, body: DynamicBody) extends Request[DynamicBody]
