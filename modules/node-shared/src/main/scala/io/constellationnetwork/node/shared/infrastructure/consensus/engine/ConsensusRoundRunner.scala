package io.constellationnetwork.node.shared.infrastructure.consensus.engine

import cats.effect.kernel.Async
import cats.kernel.Next
import cats.syntax.all._

import io.constellationnetwork.ext.cats.syntax.next.catsSyntaxNext
import io.constellationnetwork.node.shared.infrastructure.consensus._
import io.constellationnetwork.node.shared.infrastructure.consensus.engine.ConsensusCommand._
import io.constellationnetwork.node.shared.infrastructure.consensus.state.ConsensusEngineContext
import io.constellationnetwork.node.shared.infrastructure.consensus.trigger.{ConsensusTrigger, EventTrigger, TimeTrigger}
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.schema.peer.PeerId

import eu.timepit.refined.auto._

class ConsensusRoundRunner[F[_]: Async: Metrics, Event, Key: Next, Artifact, Ctx, Status, Outcome, Kind](
  ctx: ConsensusEngineContext[F, Event, Key, Artifact, Ctx, Status, Outcome, Kind]
)(
  implicit _key: monocle.Lens[Outcome, Key],
  _artifact: monocle.Lens[Outcome, io.constellationnetwork.security.signature.Signed[Artifact]],
  _context: monocle.Lens[Outcome, Ctx],
  _trigger: monocle.Lens[Outcome, ConsensusTrigger]
) {

  private val storage = ctx.storage
  private val creator = ctx.creator
  private val updater = ctx.updater
  private val advancer = ctx.advancer
  private val remover = ctx.remover
  private val logger = ctx.logger

  def runRound(trigger: ConsensusTrigger): F[Unit] =
    for {
      _ <- logger.info(s"Starting consensus round with trigger = $trigger")
      _ <- attemptRound(trigger)
      _ <- logger.info(s"Consensus round finished.")
    } yield ()

  /** Internal round execution.
    *
    * This does NOT loop recursively — it uses a tail-recursive function implemented with `flatMap` chaining to guarantee stack safety.
    */
  private def attemptRound(trigger: ConsensusTrigger): F[Unit] =
    for {
      maybeLastOutcome <- storage.getLastConsensusOutcome
      _ <- maybeLastOutcome match {
        case None =>
          logger.warn(s"No previous outcome found; cannot start consensus round.").void

        case Some(lastOutcome) =>
          val nextKey = _key.get(lastOutcome).next
          roundLoop(lastOutcome, nextKey, trigger)
      }
    } yield ()

  /** Main tail-recursive loop for a round.
    *
    * Steps per iteration:
    *   1. Get resources 2. Try facilitation (creator) 3. Try update (updater) 4. Try produce outcome (advancer) 5. If outcome exists →
    *      finish 6. If still progress → continue 7. If no progress → finish with no outcome
    */
  private def roundLoop(
    lastOutcome: Outcome,
    nextKey: Key,
    trigger: ConsensusTrigger
  ): F[Unit] = {

    def step: F[Either[Unit, Unit]] =
      for {
        resources <- storage.getResources(nextKey)
        maybeNewState <- creator.tryFacilitateConsensus(nextKey, lastOutcome, Some(trigger), resources)
        updateResult <- updater.tryUpdateConsensus(nextKey, resources)

        result <- updateResult match {
          case None =>
            maybeNewState match {
              case None =>
                logger.debug(s"No progress detected; stopping round.") *> Right(()).asInstanceOf[Either[Unit, Unit]].pure[F]
              case Some(_) =>
                logger.debug(s"Facilitation updated state; continuing round.") *> Left(()).asInstanceOf[Either[Unit, Unit]].pure[F]
            }

          case Some((oldState, newState)) =>
            advancer.getConsensusOutcome(newState) match {
              case Some((previousKey, newOutcome)) =>
                for {
                  now <- cats.effect.Clock[F].monotonic
                  _ <- Metrics[F]
                    .recordTime("dag_consensus_duration", now - newState.createdAt)

                  updated <- storage
                    .tryUpdateLastConsensusOutcomeWithCleanup(previousKey, newOutcome)

                  _ <-
                    if (updated)
                      logger.info(
                        s"Consensus reached a final outcome at key ${_key.get(newOutcome)}"
                      )
                    else
                      logger.warn("Could not update last outcome; skipping trigger after outcome.")

                  // Always end round when final outcome is produced
                } yield Right(()).asInstanceOf[Either[Unit, Unit]]

              // No outcome yet — keep looping
              case None =>
                logger.debug(s"State updated but no outcome yet; continuing round.") *>
                  Left(()).asInstanceOf[Either[Unit, Unit]].pure[F]
            }
        }
      } yield result

    // Tail recursive flatMap loop for stack-safety
    step.flatMap {
      case Left(_)  => roundLoop(lastOutcome, nextKey, trigger)
      case Right(_) => Async[F].unit // end round
    }
  }
}
