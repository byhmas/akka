/*
 * Copyright (C) 2016-2018 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.internal

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ ActorContext, StashBuffer }
import akka.actor.{ DeadLetter, StashOverflowException }
import akka.annotation.InternalApi
import akka.persistence.typed.internal.EventsourcedBehavior.InternalProtocol
import akka.persistence._
import akka.util.ConstantFun
import akka.{ actor ⇒ a }

/** INTERNAL API: Stash management for persistent behaviors */
@InternalApi
private[akka] trait EventsourcedStashManagement[C, E, S] {
  import akka.actor.typed.scaladsl.adapter._

  def setup: EventsourcedSetup[C, E, S]

  private def context: ActorContext[InternalProtocol] = setup.context

  private def internalStashBuffer: StashBuffer[InternalProtocol] = setup.internalStash

  protected def isInternalStashEmpty: Boolean = internalStashBuffer.isEmpty

  private def externalStashBuffer: StashBuffer[InternalProtocol] = setup.externalStash

  protected def stashInternal(msg: InternalProtocol): Unit =
    stash(msg, internalStashBuffer)

  protected def stashExternal(msg: InternalProtocol): Unit =
    stash(msg, externalStashBuffer)

  private def stash(msg: InternalProtocol, buffer: StashBuffer[InternalProtocol]): Unit = {
    if (setup.settings.logOnStashing) setup.log.debug("Stashing message: [{}] to {} stash", msg,
      if (buffer eq internalStashBuffer) "internal" else "external")

    try buffer.stash(msg) catch {
      case e: StashOverflowException ⇒
        setup.stashOverflowStrategy match {
          case DiscardToDeadLetterStrategy ⇒
            val noSenderBecauseAkkaTyped: a.ActorRef = a.ActorRef.noSender
            context.system.deadLetters.tell(DeadLetter(msg, noSenderBecauseAkkaTyped, context.self.toUntyped))

          case ReplyToStrategy(_) ⇒
            throw new RuntimeException("ReplyToStrategy does not make sense at all in Akka Typed, since there is no sender()!")

          case ThrowOverflowExceptionStrategy ⇒
            throw e
        }
    }
  }

  protected def tryUnstashOneInternal(
    behavior: Behavior[InternalProtocol]): Behavior[InternalProtocol] = {
    if (internalStashBuffer.nonEmpty) {
      if (setup.settings.logOnStashing) setup.log.debug(
        "Unstashing message: [{}] from internal stash",
        internalStashBuffer.head)

      internalStashBuffer.unstash(setup.context, behavior.asInstanceOf[Behavior[InternalProtocol]], 1, ConstantFun.scalaIdentityFunction)
    } else behavior
  }

  protected def tryUnstashAllExternal(
    behavior: Behavior[InternalProtocol]): Behavior[InternalProtocol] = {
    if (externalStashBuffer.nonEmpty) {
      if (setup.settings.logOnStashing) setup.log.debug("Unstashing all from external stash")
      externalStashBuffer.unstashAll(setup.context, behavior)
    } else behavior
  }

}
