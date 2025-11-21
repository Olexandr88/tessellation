package io.constellationnetwork.node.shared.infrastructure.consensus.engine

import cats.effect.kernel.Async
import cats.effect.std.{Queue, Random}
import cats.kernel.Next
import cats.syntax.all._
import fs2.Stream
import io.circe.Decoder
import io.constellationnetwork.node.shared.config.types.ConsensusConfig
import io.constellationnetwork.node.shared.domain.cluster.storage.ClusterStorage
import io.constellationnetwork.node.shared.domain.consensus.ConsensusFunctions
import io.constellationnetwork.node.shared.domain.node.NodeStorage
import io.constellationnetwork.node.shared.infrastructure.consensus._
import io.constellationnetwork.node.shared.infrastructure.consensus.state._
import io.constellationnetwork.node.shared.infrastructure.consensus.trigger._
import io.constellationnetwork.node.shared.infrastructure.gossip.RumorHandler
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.schema.node.NodeState
import io.constellationnetwork.schema.peer.Peer
import io.constellationnetwork.security.HasherSelector

object ConsensusEventLoop {

  final case class BuiltConsensusLoop[F[_], Event, Key, Artifact, Ctx, Status, Outcome, Kind](
    run    : Stream[F, Unit],
    manager: ConsensusManager[F, Event, Key, Artifact, Ctx, Status, Outcome, Kind],
    queue  : Queue[F, ConsensusCommand]
  )


  def build[
    F[_] : Async : HasherSelector : Metrics : Random,
    Event,
    Key: Next,
    Artifact,
    Ctx,
    Status,
    Outcome,
    Kind
  ](
    storage           : ConsensusStorage[F, Event, Key, Artifact, Ctx, Status, Outcome, Kind],
    stateCreator      : ConsensusStateCreator[F, Key, Artifact, Ctx, Status, Outcome, Kind],
    stateUpdater      : ConsensusStateUpdater[F, Key, Artifact, Ctx, Status, Outcome, Kind],
    stateAdvancer     : ConsensusStateAdvancer[F, Key, Artifact, Ctx, Status, Outcome, Kind],
    stateRemover      : ConsensusStateRemover[F, Key, Event, Artifact, Ctx, Status, Outcome, Kind],
    ops               : ConsensusOps[Status, Kind],
    nodeStorage       : NodeStorage[F],
    clusterStorage    : ClusterStorage[F],
    consensusFunctions: ConsensusFunctions[F, Event, Key, Artifact, Ctx],
    consensusClient   : ConsensusClient[F, Key, Outcome],
    config            : ConsensusConfig
  )(
    implicit _key: monocle.Lens[Outcome, Key],
    _context     : monocle.Lens[Outcome, Ctx],
    _artifact    : monocle.Lens[Outcome, io.constellationnetwork.security.signature.Signed[Artifact]],
    _trigger     : monocle.Lens[Outcome, ConsensusTrigger]
  ): F[BuiltConsensusLoop[F, Event, Key, Artifact, Ctx, Status, Outcome, Kind]] = {
    def collectRegistration(peer: Peer): F[Unit] =
      for {
        reg <- consensusClient.getRegistration.run(peer)
        _ <- reg.maybeKey.traverse(key =>
          storage.registerPeer(peer.id, key).void
        )
      } yield ()

    for {
      queue <- Queue.unbounded[F, ConsensusCommand]

      pending <- PendingTriggers.create[F]

      ctx <- ConsensusEngineContext.create(
        queue,
        pending,
        storage,
        stateCreator,
        stateUpdater,
        stateAdvancer,
        stateRemover,
        ops,
        nodeStorage,
        clusterStorage,
        org.typelevel.log4cats.slf4j.Slf4jLogger.getLogger[F],
        config,
        consensusFunctions,
        consensusClient
      )

      roundRunner = new ConsensusRoundRunner[F, Event, Key, Artifact, Ctx, Status, Outcome, Kind](ctx)

      scheduler <- ConsensusScheduler.make[F](queue, config.timeTriggerInterval)

      manager <- ConsensusManager.make[F, Event, Key, Artifact, Ctx, Status, Outcome, Kind](
        queue,
        storage,
        scheduler,
        nodeStorage,
        clusterStorage,
        config
      )


      engine <- ConsensusEngine
        .make[F](queue, new ConsensusFSM[F, Event, Key, Artifact, Ctx, Status, Outcome, Kind](ctx, roundRunner), scheduler)


      peerRegistrationStream: Stream[F, Unit] =
        clusterStorage.peerChanges
          .mapFilter {
            case cats.data.Ior.Both(_, peer) if peer.state === NodeState.Observing =>
              Some(peer)
            case cats.data.Ior.Right(peer) if peer.state === NodeState.Observing =>
              Some(peer)
            case _ =>
              None
          }
          .filter(_.isResponsive)
          .evalMap(collectRegistration)
          .handleErrorWith(e =>
            Stream.eval(ctx.logger.error(e)("Peer registration failed"))
          )


      leavingStream: Stream[F, Unit] =
        nodeStorage.nodeStates
          .filter(_ === NodeState.Leaving)
          .evalMap(_ => manager.withdrawFromConsensus)
          .handleErrorWith(e => Stream.eval(ctx.logger.error(e)("Error handling Leaving state")))

      run: Stream[F, Unit] =
        Stream(
          scheduler.timeTickStream,
          engine.run,
          peerRegistrationStream,
          leavingStream
        ).parJoinUnbounded

    } yield BuiltConsensusLoop(run, manager, queue)
  }
}
