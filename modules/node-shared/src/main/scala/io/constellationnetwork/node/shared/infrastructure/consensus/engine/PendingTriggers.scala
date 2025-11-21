package io.constellationnetwork.node.shared.infrastructure.consensus.engine

import cats.effect.kernel.Ref
import cats.effect.{Sync, SyncIO}
import cats.syntax.all._

final case class PendingTriggers(
  eventPending: Ref[SyncIO, Boolean],
  timePending: Ref[SyncIO, Boolean]
)

object PendingTriggers {

  def create[F[_]: Sync]: F[PendingTriggersF[F]] =
    for {
      eventRef <- Ref.of[F, Boolean](false)
      timeRef <- Ref.of[F, Boolean](false)
    } yield new PendingTriggersF[F](eventRef, timeRef)

}

final class PendingTriggersF[F[_]: Sync](
  private val eventRef: Ref[F, Boolean],
  private val timeRef: Ref[F, Boolean]
) {

  def setEvent: F[Unit] = eventRef.set(true)

  def setTime: F[Unit] = timeRef.set(true)

  def pullNext: F[Option[TriggerPriority]] =
    for {
      time <- timeRef.get
      event <- eventRef.get

      _ <- timeRef.set(false)
      _ <- eventRef.set(false)
    } yield
      if (time) Some(TriggerPriority.Time)
      else if (event) Some(TriggerPriority.Event)
      else None
}

sealed trait TriggerPriority
object TriggerPriority {
  case object Time extends TriggerPriority
  case object Event extends TriggerPriority
}
