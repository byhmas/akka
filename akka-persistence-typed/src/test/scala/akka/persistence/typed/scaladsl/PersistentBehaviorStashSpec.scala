/*
 * Copyright (C) 2017-2018 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.scaladsl

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

import akka.NotUsed
import akka.actor.testkit.typed.scaladsl._
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.typed.ExpectingReply
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.SideEffect
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.WordSpecLike

object PersistentBehaviorStashSpec {
  def conf: Config = ConfigFactory.parseString(
    """
    #akka.loglevel = DEBUG
    #akka.persistence.typed.log-stashing = on
    akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
    """)

  sealed trait Command[ReplyMessage] extends ExpectingReply[ReplyMessage]
  // Unstash and change to active mode
  final case class Activate(id: String, override val replyTo: ActorRef[Ack]) extends Command[Ack]
  // Change to active mode, stash incoming Increment
  final case class Deactivate(id: String, override val replyTo: ActorRef[Ack]) extends Command[Ack]
  // Persist Incremented if in active mode, otherwise stashed
  final case class Increment(id: String, override val replyTo: ActorRef[Ack]) extends Command[Ack]
  // Persist ValueUpdated, independent of active/inactive
  final case class UpdateValue(id: String, value: Int, override val replyTo: ActorRef[Ack]) extends Command[Ack]
  // Retrieve current state, independent of active/inactive
  final case class GetValue(replyTo: ActorRef[State]) extends Command[State]
  final case class Unhandled(replyTo: ActorRef[NotUsed]) extends Command[NotUsed]

  final case class Ack(id: String)

  sealed trait Event
  final case class Incremented(delta: Int) extends Event
  final case class ValueUpdated(value: Int) extends Event
  case object Activated extends Event
  case object Deactivated extends Event

  final case class State(value: Int, history: Vector[Int], active: Boolean)

  def counter(persistenceId: PersistenceId): Behavior[Command[_]] =
    Behaviors.setup(ctx ⇒ counter(ctx, persistenceId))

  def counter(
    ctx:           ActorContext[Command[_]],
    persistenceId: PersistenceId): PersistentBehavior[Command[_], Event, State] = {
    PersistentBehavior.withEnforcedReplies[Command[_], Event, State](
      persistenceId,
      emptyState = State(0, Vector.empty, active = true),
      commandHandler = (state, command) ⇒ {
        if (state.active) active(state, command)
        else inactive(state, command)
      },
      eventHandler = (state, evt) ⇒ evt match {
        case Incremented(delta) ⇒
          //          if (!state.active) throw new IllegalStateException
          State(state.value + delta, state.history :+ state.value, active = true)
        case ValueUpdated(value) ⇒
          State(value, state.history :+ state.value, active = state.active)
        case Activated ⇒
          if (state.active) throw new IllegalStateException
          state.copy(active = true)
        case Deactivated ⇒
          if (!state.active) throw new IllegalStateException
          state.copy(active = false)
      })
  }

  private def active(state: State, command: Command[_]): ReplyEffect[Event, State] = {
    command match {
      case cmd: Increment ⇒
        Effect.persist(Incremented(1))
          .thenReply(cmd)(_ ⇒ Ack(cmd.id))
      case cmd @ UpdateValue(_, value, _) ⇒
        Effect.persist(ValueUpdated(value))
          .thenReply(cmd)(_ ⇒ Ack(cmd.id))
      case query: GetValue ⇒
        Effect.reply(query)(state)
      case cmd: Deactivate ⇒
        Effect.persist(Deactivated)
          .thenReply(cmd)(_ ⇒ Ack(cmd.id))
      case cmd: Activate ⇒
        // already active
        Effect.reply(cmd)(Ack(cmd.id))
      case _: Unhandled ⇒
        Effect.unhandled.thenNoReply()
    }
  }

  private def inactive(state: State, command: Command[_]): ReplyEffect[Event, State] = {
    command match {
      case cmd: Increment ⇒
        Effect.stash()
      case cmd @ UpdateValue(_, value, _) ⇒
        Effect.persist(ValueUpdated(value))
          .thenReply(cmd)(_ ⇒ Ack(cmd.id))
      case query: GetValue ⇒
        Effect.reply(query)(state)
      case cmd: Deactivate ⇒
        // already inactive
        Effect.reply(cmd)(Ack(cmd.id))
      case cmd: Activate ⇒
        Effect.persist(Activated)
          .andThen(SideEffect.unstashAll[State]()) // FIXME perhaps we need `.thenUnstashAll`
          .thenReply(cmd)(_ ⇒ Ack(cmd.id))
      case _: Unhandled ⇒
        Effect.unhandled.thenNoReply()
    }
  }
}

class PersistentBehaviorStashSpec extends ScalaTestWithActorTestKit(PersistentBehaviorStashSpec.conf) with WordSpecLike {

  import PersistentBehaviorStashSpec._

  val pidCounter = new AtomicInteger(0)
  private def nextPid(): PersistenceId = PersistenceId(s"c${pidCounter.incrementAndGet()})")

  "A typed persistent actor that is stashing commands" must {

    "stash and unstash" in {
      val c = spawn(counter(nextPid()))
      val ackProbe = TestProbe[Ack]
      val stateProbe = TestProbe[State]

      c ! Increment("1", ackProbe.ref)
      ackProbe.expectMessage(Ack("1"))

      c ! Deactivate("2", ackProbe.ref)
      ackProbe.expectMessage(Ack("2"))

      c ! Increment("3", ackProbe.ref)
      c ! Increment("4", ackProbe.ref)
      c ! GetValue(stateProbe.ref)
      stateProbe.expectMessage(State(1, Vector(0), active = false))

      c ! Activate("5", ackProbe.ref)
      c ! Increment("6", ackProbe.ref)

      ackProbe.expectMessage(Ack("5"))
      ackProbe.expectMessage(Ack("3"))
      ackProbe.expectMessage(Ack("4"))
      ackProbe.expectMessage(Ack("6"))

      c ! GetValue(stateProbe.ref)
      stateProbe.expectMessage(State(4, Vector(0, 1, 2, 3), active = true))
    }

    "handle mix of stash, persist and unstash" in {
      val c = spawn(counter(nextPid()))
      val ackProbe = TestProbe[Ack]
      val stateProbe = TestProbe[State]

      c ! Increment("1", ackProbe.ref)
      ackProbe.expectMessage(Ack("1"))

      c ! Deactivate("2", ackProbe.ref)
      ackProbe.expectMessage(Ack("2"))

      c ! Increment("3", ackProbe.ref)
      c ! Increment("4", ackProbe.ref)
      // UpdateValue will persist when inactive (with previously stashed commands)
      c ! UpdateValue("5", 100, ackProbe.ref)
      ackProbe.expectMessage(Ack("5"))

      c ! GetValue(stateProbe.ref)
      stateProbe.expectMessage(State(100, Vector(0, 1), active = false))

      c ! Activate("6", ackProbe.ref)
      c ! GetValue(stateProbe.ref)
      stateProbe.expectMessage(State(102, Vector(0, 1, 100, 101), active = true))
      ackProbe.expectMessage(Ack("6"))
      ackProbe.expectMessage(Ack("3"))
      ackProbe.expectMessage(Ack("4"))
    }

    "unstash in right order" in {
      val c = spawn(counter(nextPid()))
      val ackProbe = TestProbe[Ack]
      val stateProbe = TestProbe[State]

      c ! Increment(s"inc-1", ackProbe.ref)

      c ! Deactivate(s"deact", ackProbe.ref)

      c ! Increment(s"inc-2", ackProbe.ref)
      c ! Increment(s"inc-3", ackProbe.ref)
      c ! Increment(s"inc-4", ackProbe.ref)

      c ! Activate(s"act", ackProbe.ref)

      c ! Increment(s"inc-5", ackProbe.ref)
      c ! Increment(s"inc-6", ackProbe.ref)
      c ! Increment(s"inc-7", ackProbe.ref)

      c ! GetValue(stateProbe.ref)
      val finalState = stateProbe.expectMessageType[State](5.seconds)

      // verify the order

      ackProbe.expectMessage(Ack("inc-1"))
      ackProbe.expectMessage(Ack("deact"))
      ackProbe.expectMessage(Ack("act"))
      ackProbe.expectMessage(Ack("inc-2"))
      ackProbe.expectMessage(Ack("inc-3"))
      ackProbe.expectMessage(Ack("inc-4"))
      ackProbe.expectMessage(Ack("inc-5"))
      ackProbe.expectMessage(Ack("inc-6"))
      ackProbe.expectMessage(Ack("inc-7"))

      finalState.value should ===(7)
    }

    "handle many stashed" in {
      val c = spawn(counter(nextPid()))
      val ackProbe = TestProbe[Ack]
      val stateProbe = TestProbe[State]
      val notUsedProbe = TestProbe[NotUsed]

      (1 to 100).foreach { n ⇒
        c ! Increment(s"inc-1-$n", ackProbe.ref)
      }

      (1 to 3).foreach { n ⇒
        c ! Deactivate(s"deact-2-$n", ackProbe.ref)
      }

      (1 to 100).foreach { n ⇒
        if (n % 10 == 0)
          c ! Unhandled(notUsedProbe.ref)
        c ! Increment(s"inc-3-$n", ackProbe.ref)
      }

      c ! GetValue(stateProbe.ref)

      (1 to 5).foreach { n ⇒
        c ! UpdateValue(s"upd-4-$n", n * 1000, ackProbe.ref)
      }

      (1 to 3).foreach { n ⇒
        c ! Activate(s"act-5-$n", ackProbe.ref)
      }

      (1 to 100).foreach { n ⇒
        c ! Increment(s"inc-6-$n", ackProbe.ref)
      }

      c ! GetValue(stateProbe.ref)

      (6 to 8).foreach { n ⇒
        c ! UpdateValue(s"upd-7-$n", n * 1000, ackProbe.ref)
      }

      (1 to 3).foreach { n ⇒
        c ! Deactivate(s"deact-8-$n", ackProbe.ref)
      }

      (1 to 100).foreach { n ⇒
        c ! Increment(s"inc-9-$n", ackProbe.ref)
      }

      (1 to 3).foreach { n ⇒
        c ! Activate(s"act-10-$n", ackProbe.ref)
      }

      c ! GetValue(stateProbe.ref)
      val value1 = stateProbe.expectMessageType[State](5.seconds).value
      val value2 = stateProbe.expectMessageType[State](5.seconds).value
      val value3 = stateProbe.expectMessageType[State](5.seconds).value

      // verify the order

      (1 to 100).foreach { n ⇒
        ackProbe.expectMessage(Ack(s"inc-1-$n"))
      }

      (1 to 3).foreach { n ⇒
        ackProbe.expectMessage(Ack(s"deact-2-$n"))
      }

      (1 to 5).foreach { n ⇒
        ackProbe.expectMessage(Ack(s"upd-4-$n"))
      }

      ackProbe.expectMessage(Ack("act-5-1"))

      (1 to 100).foreach { n ⇒
        ackProbe.expectMessage(Ack(s"inc-3-$n"))
      }

      (2 to 3).foreach { n ⇒
        ackProbe.expectMessage(Ack(s"act-5-$n"))
      }

      (1 to 100).foreach { n ⇒
        ackProbe.expectMessage(Ack(s"inc-6-$n"))
      }

      (6 to 8).foreach { n ⇒
        ackProbe.expectMessage(Ack(s"upd-7-$n"))
      }

      (1 to 3).foreach { n ⇒
        ackProbe.expectMessage(Ack(s"deact-8-$n"))
      }

      ackProbe.expectMessage(Ack("act-10-1"))

      (1 to 100).foreach { n ⇒
        ackProbe.expectMessage(Ack(s"inc-9-$n"))
      }

      (2 to 3).foreach { n ⇒
        ackProbe.expectMessage(Ack(s"act-10-$n"))
      }

      value1 should ===(100)
      value2 should ===(5200)
      value3 should ===(8100)
    }
  }

  // FIXME test combination with PoisonPill

}
