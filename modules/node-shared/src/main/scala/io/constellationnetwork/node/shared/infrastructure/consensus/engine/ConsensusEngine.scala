package io.constellationnetwork.node.shared.infrastructure.consensus.engine

import cats.effect.kernel.Async
import cats.effect.std.Queue
import cats.syntax.all._

import io.constellationnetwork.node.shared.infrastructure.consensus.engine.ConsensusCommand._
import io.constellationnetwork.node.shared.infrastructure.consensus.state.ConsensusFSM

import fs2.Stream

/** ConsensusEngine ties everything together:
  *
  *   - The queue (FIFO)
  *   - FSM (interprets commands sequentially)
  *   - Scheduler (time ticks + delayed tasks)
  *   - FS2 loop that processes commands forever
  *
  * All consensus actions are serialized by this file.
  */
class ConsensusEngine[F[_]: Async](
  queue: Queue[F, ConsensusCommand],
  fsm: ConsensusFSM[F, _, _, _, _, _, _, _],
  scheduler: ConsensusScheduler[F]
) {

  /** The main engine loop: Reads commands from queue one-by-one and delegates to FSM.
    */
  private val commandStream: Stream[F, Unit] =
    Stream
      .repeatEval(queue.take)
      .evalMap(cmd => fsm.handle(cmd))

  /** Combined engine stream:
    *   - scheduler.timeTickStream emits TimeTick periodically
    *   - commandStream processes commands from queue
    */
  val run: Stream[F, Unit] =
    Stream(
      scheduler.timeTickStream,
      commandStream
    ).parJoinUnbounded
}

object ConsensusEngine {

  /** Create a new engine.
    *
    * @param queue
    *   main work queue
    * @param fsm
    *   finite-state machine
    * @param scheduler
    *   time/delayed command scheduler
    */
  def make[F[_]: Async](
    queue: Queue[F, ConsensusCommand],
    fsm: ConsensusFSM[F, _, _, _, _, _, _, _],
    scheduler: ConsensusScheduler[F]
  ): F[ConsensusEngine[F]] =
    Async[F].pure(new ConsensusEngine[F](queue, fsm, scheduler))
}
