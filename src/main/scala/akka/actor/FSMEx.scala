package akka.actor

trait FSMEx[S, D] extends FSM[S, D] { this: Actor ⇒
  private[akka] override def processEvent(event: Event, source: AnyRef): Unit = {
    processEventEx(event, source)
  }

  protected def processEventEx(event: Event, source: AnyRef): Unit = {
    super.processEvent(event,source)
  }
}
