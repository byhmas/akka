/*
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.internal

import akka.actor.typed.scaladsl.StashBuffer
import akka.annotation.InternalApi
import akka.persistence.typed.internal.EventsourcedBehavior.InternalProtocol
import akka.util.OptionVal

/**
 * INTERNAL API
 * Main reason for introduction of this trait is stash buffer reference management
 * in order to survive restart of internal behavior
 */
@InternalApi private[akka] trait EventsourcedStashReferenceManagement {

  private var internalStashBuffer: OptionVal[StashBuffer[InternalProtocol]] = OptionVal.None
  private var externalStashBuffer: OptionVal[StashBuffer[InternalProtocol]] = OptionVal.None

  def internalStashBuffer(settings: EventsourcedSettings): StashBuffer[InternalProtocol] = {
    val buffer: StashBuffer[InternalProtocol] = internalStashBuffer match {
      case OptionVal.Some(value) ⇒ value
      case _                     ⇒ StashBuffer(settings.stashCapacity)
    }
    this.internalStashBuffer = OptionVal.Some(buffer)
    internalStashBuffer.get
  }

  def externalStashBuffer(settings: EventsourcedSettings): StashBuffer[InternalProtocol] = {
    val buffer: StashBuffer[InternalProtocol] = externalStashBuffer match {
      case OptionVal.Some(value) ⇒ value
      case _                     ⇒ StashBuffer(settings.stashCapacity)
    }
    this.externalStashBuffer = OptionVal.Some(buffer)
    externalStashBuffer.get
  }

  def clearStashBuffers(): Unit = {
    internalStashBuffer = OptionVal.None
    externalStashBuffer = OptionVal.None
  }
}
