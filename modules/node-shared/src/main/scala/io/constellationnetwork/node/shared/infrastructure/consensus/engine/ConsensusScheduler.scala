package io.constellationnetwork.node.shared.infrastructure.consensus.engine

import cats.effect.kernel.{Async, Spawn, Temporal}
import cats.effect.std.Queue
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.node.shared.infrastructure.consensus.engine.ConsensusCommand._
import io.constellationnetwork.node.shared.infrastructure.consensus.trigger._

import fs2.Stream

/** ConsensusScheduler:
  *
  *   - Sends periodic TIME ticks into the queue using Stream.awakeEvery
  *   - Schedules delayed commands (timeouts, stall detection, etc.) by using Spawn.start { sleep >> queue.offer(...) }
  *
  * This keeps the engine responsive and non-blocking.
  */
class ConsensusScheduler[F[_]: Async](
  queue: Queue[F, ConsensusCommand],
  timeTriggerInterval: FiniteDuration
) {

  /** Periodic stream emitting TimeTick. */
  val timeTickStream: Stream[F, Unit] =
    Stream
      .awakeEvery[F](timeTriggerInterval)
      .evalMap(_ => queue.offer(TimeTick))

  /** Schedule a command AFTER a delay. This uses Spawn.start so it never blocks the engine.
    */
  def scheduleAfter(delay: FiniteDuration)(cmd: ConsensusCommand): F[Unit] =
    Spawn[F].start {
      Temporal[F].sleep(delay) >>
        queue.offer(InternalScheduled(cmd))
    }.void

  /** Convenience method for scheduling a StartRound(trigger) after a delay.
    */
  def scheduleRoundAfter(delay: FiniteDuration, trigger: ConsensusTrigger): F[Unit] =
    scheduleAfter(delay)(StartRound(trigger))
}

object ConsensusScheduler {
  def make[F[_]: Async](
    queue: Queue[F, ConsensusCommand],
    timeTriggerInterval: FiniteDuration
  ): F[ConsensusScheduler[F]] =
    Async[F].pure(new ConsensusScheduler[F](queue, timeTriggerInterval))
}
