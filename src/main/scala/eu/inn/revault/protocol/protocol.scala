package eu.inn.revault.protocol

import eu.inn.binders.dynamic.Value
import eu.inn.hyperbus.model._
import eu.inn.hyperbus.model.annotations.{body, request}
import eu.inn.hyperbus.model.standard._

@body("revault-query")
case class Query(path: String)

@request("/revault/{path}") // todo: path can include /
case class RevaultGet(body: Query) extends StaticGet(body)
with DefinedResponse[Ok[DynamicBody]]

@body("revault-monitor")
case class Monitor(id: String, status: String) extends Links

@request("/revault/{path}")
case class RevaultPut(body: DynamicBody) extends StaticPut(body)
with DefinedResponse[(
    Created[CreatedBody],
    Accepted[Monitor],
    NoContent[EmptyBody]
  )]

@request("/revault/{path}")
case class RevaultPatch(body: DynamicBody) extends StaticPatch(body)
with DefinedResponse[(
    Accepted[Monitor],
    NoContent[EmptyBody]
  )]

@request("/revault/{path}")
case class RevaultDelete(body: Query) extends StaticDelete(body)
with DefinedResponse[(
    Accepted[Monitor],
    NoContent[EmptyBody]
  )]

// collection is specified by _links.parent -> ... parent
