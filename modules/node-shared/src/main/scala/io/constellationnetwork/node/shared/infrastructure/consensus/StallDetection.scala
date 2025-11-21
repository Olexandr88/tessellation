package io.constellationnetwork.node.shared.infrastructure.consensus

import cats._
import cats.effect._
import cats.effect.std.Supervisor
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.node.shared.config.types.ConsensusConfig
import io.constellationnetwork.node.shared.domain.cluster.storage.ClusterStorage

import fs2.concurrent.SignallingRef
import org.typelevel.log4cats.Logger

private[consensus] object StallDetection {

  def scheduleStallDetection[
    F[_]: Temporal,
    Key,
    Artifact,
    Context,
    Status,
    OutcomeC,
    Kind
  ](
    key: Key,
    config: ConsensusConfig,
    stallDetectionRef: SignallingRef[F, Map[Key, Long]],
    consensusOps: ConsensusOps[Status, Kind],
    consensusStorage: ConsensusStorage[F, _, Key, Artifact, Context, Status, OutcomeC, Kind],
    consensusStateUpdater: ConsensusStateUpdater[F, Key, Artifact, Context, Status, OutcomeC, Kind],
    clusterStorage: ClusterStorage[F],
    queue: ConsensusQueue[F, Key],
    logger: Logger[F],
    isFirstRoundAfterJoin: Boolean
  )(implicit S: Supervisor[F]): F[Unit] =
    Clock[F].monotonic.flatMap { now =>
      val stallId = now.toMillis

      stallDetectionRef.update(_ + (key -> stallId)) >>
        S.supervise {
          val sleepDuration = if (isFirstRoundAfterJoin) {
            60.seconds
          } else {
            config.declarationTimeout
          }
          Temporal[F].sleep(sleepDuration) >>
            stallDetectionRef.get.flatMap { currentMap =>
              if (currentMap.get(key).contains(stallId)) {
                logger.warn(s"Stall detected for consensus round {key=${key.toString}}") >>
                  processStallDetection(
                    key,
                    config,
                    consensusOps,
                    consensusStorage,
                    consensusStateUpdater,
                    clusterStorage,
                    queue,
                    logger
                  ) >>
                  stallDetectionRef.update(_ - key)
              } else {
                Applicative[F].unit
              }
            }.handleErrorWith { err =>
              logger.error(err)(s"Error in stall detection {key=${key.toString}}") >>
                stallDetectionRef.update(_ - key)
            }
        }.void
    }

  private[consensus] def processStallDetection[
    F[_]: Temporal,
    Key,
    Artifact,
    Context,
    Status,
    OutcomeC,
    Kind
  ](
    key: Key,
    config: ConsensusConfig,
    consensusOps: ConsensusOps[Status, Kind],
    consensusStorage: ConsensusStorage[F, _, Key, Artifact, Context, Status, OutcomeC, Kind],
    consensusStateUpdater: ConsensusStateUpdater[F, Key, Artifact, Context, Status, OutcomeC, Kind],
    clusterStorage: ClusterStorage[F],
    queue: ConsensusQueue[F, Key],
    logger: Logger[F]
  ): F[Unit] =
    consensusStorage.getState(key).flatMap {
      case Some(currentState) =>
        consensusStateUpdater
          .tryLockConsensus(key, currentState)
          .flatMap {
            case Some((_, lockedState)) =>
              logger.info(s"Consensus locked for stall recovery {key=${key.toString}}") >>
                Temporal[F].sleep(config.lockDuration) >>
                consensusOps
                  .maybeCollectingKind(lockedState.status)
                  .traverse { ackKind =>
                    for {
                      resources <- consensusStorage.getResources(key)
                      responsivePeerIds <- clusterStorage.getResponsivePeers.map(_.map(_.id))
                      filteredResources = resources.copy(
                        peerDeclarationsMap = resources.peerDeclarationsMap.filter { case (peerId, _) =>
                          responsivePeerIds.contains(peerId)
                        },
                        acksMap = resources.acksMap.filter { case ((peerId, _), _) =>
                          responsivePeerIds.contains(peerId)
                        }
                      )
                      removedCount = resources.peerDeclarationsMap.size - filteredResources.peerDeclarationsMap.size
                      _ <- logger.info(
                        s"Filtered out $removedCount unresponsive peers for stall recovery {key=${key.toString}, " +
                        s"total=${resources.peerDeclarationsMap.size}, responsive=${filteredResources.peerDeclarationsMap.size}}"
                      )
                      _ <- consensusStateUpdater.trySpreadAck(key, ackKind, filteredResources)
                      _ <- logger.debug(s"ACKs spread for stall recovery {key=${key.toString}, kind=${ackKind.toString}}")
                      _ <- logger.info(s"Requesting state update after spreading ACKs {key=${key.toString}}")
                      _ <- queue.requestStateUpdate(key)
                    } yield ()
                  }
                  .void

            case None =>
              logger.debug(s"Could not lock consensus for stall recovery {key=${key.toString}}")
          }
      case None =>
        logger.debug(s"State no longer exists {key=${key.toString}}")
    }
}
