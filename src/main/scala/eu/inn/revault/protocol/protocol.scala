package eu.inn.revault.protocol

import java.util.Date

import eu.inn.hyperbus.model._
import eu.inn.hyperbus.model.annotations.{body, request}
import eu.inn.hyperbus.model.standard._

@request("/revault/{path:*}") // todo: check if raml arg can be with pattern
case class RevaultGet(path: String, body: EmptyBody) extends StaticGet(body)
with DefinedResponse[Ok[DynamicBody]]

@body("revault-monitor")
case class Monitor(id: String, status: String, completedAt: Option[Date]) extends Body

@request("/revault/{path:*}")
case class RevaultPut(path: String, body: DynamicBody) extends StaticPut(body)
with DefinedResponse[(
    //Created[DynamicBody with CreatedBody],
    Accepted[Monitor],
    NoContent[EmptyBody]
  )]

@request("/revault/{path:*}/feed")
case class RevaultFeedPut(path: String, body: DynamicBody) extends StaticPut(body)

@request("/revault/{path:*}")
case class RevaultPatch(path: String, body: DynamicBody) extends StaticPatch(body)
  with DefinedResponse[(
      Accepted[Monitor],
      NoContent[EmptyBody]
    )]

@request("/revault/{path:*}/feed")
case class RevaultFeedPatch(path: String, body: DynamicBody) extends StaticPatch(body)

@request("/revault/{path:*}")
case class RevaultDelete(path: String, body: EmptyBody) extends StaticDelete(body)
  with DefinedResponse[(
      Accepted[Monitor],
      NoContent[EmptyBody]
    )]

@request("/revault/{path:*}/feed")
case class RevaultFeedDelete(path: String, body: EmptyBody) extends StaticDelete(body)

/*

*/
// collection is specified by _links.parent -> ... parent
