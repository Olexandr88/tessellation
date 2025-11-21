package io.constellationnetwork.node.shared.infrastructure.consensus.engine

import cats.effect.kernel.Async
import cats.effect.std.Queue
import cats.kernel.Next
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.ext.cats.syntax.next.catsSyntaxNext
import io.constellationnetwork.node.shared.config.types.ConsensusConfig
import io.constellationnetwork.node.shared.domain.cluster.storage.ClusterStorage
import io.constellationnetwork.node.shared.domain.node.NodeStorage
import io.constellationnetwork.node.shared.infrastructure.consensus._
import io.constellationnetwork.node.shared.infrastructure.consensus.engine.ConsensusCommand._
import io.constellationnetwork.node.shared.infrastructure.consensus.state._
import io.constellationnetwork.node.shared.infrastructure.consensus.trigger._
import io.constellationnetwork.schema.node.NodeState
import io.constellationnetwork.schema.peer.Peer
import io.constellationnetwork.security.signature.Signed

class ConsensusManager[F[_]: Async, Event, Key: Next, Artifact, Ctx, Status, Outcome, Kind](
  queue: Queue[F, ConsensusCommand],
  storage: ConsensusStorage[F, Event, Key, Artifact, Ctx, Status, Outcome, Kind],
  scheduler: ConsensusScheduler[F],
  nodeStorage: NodeStorage[F],
  clusterStorage: ClusterStorage[F],
  config: ConsensusConfig
)(
  implicit _key: monocle.Lens[Outcome, Key],
  _context: monocle.Lens[Outcome, Ctx],
  _artifact: monocle.Lens[Outcome, io.constellationnetwork.security.signature.Signed[Artifact]],
  _trigger: monocle.Lens[Outcome, ConsensusTrigger]
) {

  // ---------------------------------------------------------------------------
  // Exposed public API
  // ---------------------------------------------------------------------------

  /** Begin participating in a consensus round at a given key. */
  def registerForConsensus(key: Key): F[Unit] =
    storage.trySetObservationKey(key).flatMap {
      case true =>
        // Force node state transition into Observing
        nodeStorage.tryModifyState(NodeState.WaitingForObserving, NodeState.Observing).void

      case false =>
        new Throwable(s"Node already registered for another consensus key").raiseError[F, Unit]
    }

  def startFacilitatingAfterDownload(
    key: Key,
    lastArtifact: Signed[Artifact],
    lastContext: Ctx
  ): F[Unit] =
    queue.offer(
      InitializeFromDownload(key, lastArtifact, lastContext)
    )

  /** OLD API: Start after rollback. FS2 version: push proper command. */
  def startFacilitatingAfterRollback(
    lastKey: Key,
    initialOutcome: Outcome
  ): F[Unit] =
    queue.offer(
      InitializeFromRollback(lastKey, initialOutcome)
    )
  
  /** Node voluntarily leaves consensus. */
  def withdrawFromConsensus: F[Unit] =
    for {
      maybeOutcome <- storage.clearAndGetLastConsensusOutcome
      _ <- maybeOutcome.traverse(out =>
        storage.tryUpdateLastConsensusOutcomeWithCleanup(
          Previous(_key.get(out).next),
          out
        )
      )
      _ <- storage.clearObservationKey
      _ <- nodeStorage.tryModifyState(NodeState.Ready, NodeState.Leaving).void
    } yield ()
  
}

object ConsensusManager {

  def make[F[_]: Async, Event, Key: Next, Artifact, Ctx, Status, Outcome, Kind](
    queue: Queue[F, ConsensusCommand],
    storage: ConsensusStorage[F, Event, Key, Artifact, Ctx, Status, Outcome, Kind],
    scheduler: ConsensusScheduler[F],
    nodeStorage: NodeStorage[F],
    clusterStorage: ClusterStorage[F],
    config: ConsensusConfig
  )(
    implicit _key: monocle.Lens[Outcome, Key],
    _context: monocle.Lens[Outcome, Ctx],
    _artifact: monocle.Lens[Outcome, io.constellationnetwork.security.signature.Signed[Artifact]],
    _trigger: monocle.Lens[Outcome, ConsensusTrigger]
  ): F[ConsensusManager[F, Event, Key, Artifact, Ctx, Status, Outcome, Kind]] =
    Async[F].pure(
      new ConsensusManager[F, Event, Key, Artifact, Ctx, Status, Outcome, Kind](
        queue,
        storage,
        scheduler,
        nodeStorage,
        clusterStorage,
        config
      )
    )
}
