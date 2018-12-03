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

  // FIXME keep state when restarting?
  private var unstashAllExternalInProgress = 0

  /**
   * Stash a command to the internal stash buffer, which is used while waiting for persist to be completed.
   */
  protected def stashInternal(msg: InternalProtocol): Unit =
    stash(msg, internalStashBuffer)

  /**
   * Stash a command to the external stash buffer, which is used when `Stash` effect is used.
   */
  protected def stashExternal(msg: InternalProtocol): Unit =
    stash(msg, externalStashBuffer)

  private def stash(msg: InternalProtocol, buffer: StashBuffer[InternalProtocol]): Unit = {
    if (setup.settings.logOnStashing) setup.log.debug(
      "Stashing message to {} stash: [{}] ",
      if (buffer eq internalStashBuffer) "internal" else "external", msg)

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

  /**
   * `tryUnstashOne` is called at the end of processing each command (or when persist is completed)
   */
  protected def tryUnstashOne(behavior: Behavior[InternalProtocol]): Behavior[InternalProtocol] = {
    val buffer =
      if (unstashAllExternalInProgress > 0)
        externalStashBuffer
      else
        internalStashBuffer

    if (buffer.nonEmpty) {
      if (setup.settings.logOnStashing) setup.log.debug(
        "Unstashing message from {} stash: [{}]",
        if (buffer eq internalStashBuffer) "internal" else "external", buffer.head)

      if (unstashAllExternalInProgress > 0)
        unstashAllExternalInProgress -= 1

      buffer.unstash(setup.context, behavior, 1, ConstantFun.scalaIdentityFunction)
    } else behavior

  }

  /**
   * Subsequent `tryUnstashOne` will first drain the external stash buffer, before using the
   * internal stash buffer. It will unstash as many commands as are in the buffer when
   * `unstashAllExternal` was called, i.e. if subsequent commands stash more those will
   * not be unstashed until `unstashAllExternal` is called again.
   */
  protected def unstashAllExternal(): Unit = {
    if (externalStashBuffer.nonEmpty) {
      if (setup.settings.logOnStashing) setup.log.debug(
        "Unstashing all [{}] messages from external stash, first is: [{}]",
        externalStashBuffer.size, externalStashBuffer.head)
      unstashAllExternalInProgress = externalStashBuffer.size
      // tryUnstashOne is called from EventSourcedRunning at the end of processing each command (or when persist is completed)
    }
  }

}
