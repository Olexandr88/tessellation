package io.constellationnetwork.node.shared.infrastructure.consensus.state

import cats.effect.kernel.Async
import cats.effect.std.Random
import cats.kernel.Next
import cats.syntax.all._
import io.constellationnetwork.ext.cats.syntax.next.catsSyntaxNext
import io.constellationnetwork.node.shared.infrastructure.consensus.declaration._
import io.constellationnetwork.node.shared.infrastructure.consensus.engine.ConsensusCommand._
import io.constellationnetwork.node.shared.infrastructure.consensus.engine._
import io.constellationnetwork.node.shared.infrastructure.consensus.message._
import io.constellationnetwork.node.shared.infrastructure.consensus.trigger._
import io.constellationnetwork.schema.gossip.{CommonRumor, Ordinal, PeerRumor}
import io.constellationnetwork.schema.node.NodeState
import io.constellationnetwork.schema.peer.{Peer, PeerId}
import io.constellationnetwork.security.HasherSelector
import io.constellationnetwork.security.signature.Signed
import monocle.Lens
import retry.RetryPolicies.{constantDelay, limitRetries}
import retry.syntax.all._

import scala.concurrent.duration._

/** ConsensusFSM interprets ConsensusCommands.
 *
 * This version is rewritten to properly handle PeerRumor / CommonRumor and fix all the errors you had in your earlier file.
 */
class ConsensusFSM[F[_] : Async : HasherSelector : Random, Event, Key: Next, Artifact, Ctx, Status, Outcome, Kind](
  ctx        : ConsensusEngineContext[F, Event, Key, Artifact, Ctx, Status, Outcome, Kind],
  roundRunner: ConsensusRoundRunner[F, Event, Key, Artifact, Ctx, Status, Outcome, Kind]
)(
  implicit _key: monocle.Lens[Outcome, Key],
  _trigger     : Lens[Outcome, ConsensusTrigger],
  _artifact    : monocle.Lens[Outcome, Signed[Artifact]],
  _context     : monocle.Lens[Outcome, Ctx]
) {

  private val log = ctx.logger
  private val pending = ctx.pending
  private val storage = ctx.storage
  private val isRunning = ctx.isRoundRunning
  private val fns = ctx.fns // ConsensusFunctions (added to context)

  /** Top-level dispatcher */
  def handle(cmd: ConsensusCommand): F[Unit] =
    isRunning.get.flatMap {
      case false => handleWhenIdle(cmd)
      case true => handleWhenBusy(cmd)
    }

  // ===========================================================================
  // IDLE
  // ===========================================================================

  private def handleWhenIdle(cmd: ConsensusCommand): F[Unit] =
    cmd match {
      case RumorReceived(r) =>
        r match {
          case Left(pr) => applyRumor(pr)
          case Right(cr) => applyRumor(cr)
        }

      case StartRound(trigger) =>
        startRound(trigger)

      case TimeTick =>
        startRound(TimeTrigger)

      case FacilitateByEvent =>
        startRound(EventTrigger)

      case CheckUpdate(key) =>
        doCheckUpdate(key.asInstanceOf[Key])

      case RoundCompleted =>
        log.warn("Received RoundCompleted while idle; ignoring.")

      case InternalScheduled(inner) =>
        handle(inner)

      case PeerObserved(peer) =>
        registerPeer(peer)

      case InitializeFromDownload(key, art, ctx) =>
        handleInitializeFromDownload(key.asInstanceOf[Key], art.asInstanceOf[Signed[Artifact]], ctx.asInstanceOf[Ctx])

      case InitializeFromRollback(key, outcome) =>
        handleInitializeFromRollback(key.asInstanceOf[Key], outcome.asInstanceOf[Outcome])

      case WithdrawFromConsensus =>
        handleWithdraw

      case IgnoreUnexpectedRumor(unexpectedRumor) => log.warn(s"Ignoring unexpected rumor: $unexpectedRumor")
    }

  // ===========================================================================
  // BUSY
  // ===========================================================================

  private def handleWhenBusy(cmd: ConsensusCommand): F[Unit] =
    cmd match {
      case RumorReceived(r) =>
        r match {
          case Left(pr) => applyRumor(pr)
          case Right(cr) => applyRumor(cr)
        }

      case CheckUpdate(key) =>
        doCheckUpdate(key.asInstanceOf[Key])

      case FacilitateByEvent =>
        pending.setEvent

      case TimeTick =>
        pending.setTime

      case StartRound(trigger) =>
        trigger match {
          case TimeTrigger => pending.setTime
          case EventTrigger => pending.setEvent
        }

      case RoundCompleted =>
        handleRoundCompleted

      case InternalScheduled(inner) =>
        handle(inner)

      case PeerObserved(peer) =>
        registerPeer(peer)

      case InitializeFromDownload(_, _, _) => ().pure

      case InitializeFromRollback(_, _) => ().pure

      case WithdrawFromConsensus => pending.setEvent

      case IgnoreUnexpectedRumor(unexpectedRumor) => log.warn(s"Ignoring unexpected rumor: $unexpectedRumor")
    }

  private def doStart(trigger: ConsensusTrigger): F[Unit] =
    for {
      _ <- log.info(s"Starting consensus round with trigger = $trigger")
      _ <- isRunning.set(true)
      _ <- roundRunner.runRound(trigger)
      _ <- ctx.queue.offer(RoundCompleted)
    } yield ()

  private def startRound(trigger: ConsensusTrigger): F[Unit] =
    for {
      running <- isRunning.get
      _ <-
        if (running)
          log.debug(s"Ignoring StartRound($trigger) — round already running")
        else
          doStart(trigger)
    } yield ()

  private def applyRumor(raw: Any): F[Unit] =
    raw match {

      case PeerRumor(origin, ordinal, content) =>
        applyPeerRumor(origin, ordinal, content)

      case CommonRumor(content) =>
        applyCommonRumor(content)

      case other =>
        log.warn(s"Unknown rumor wrapper: $other")
    }

  private def applyPeerRumor(
    origin : PeerId,
    ordinal: Ordinal,
    content: Any
  ): F[Unit] = content match {

    case ConsensusPeerDeclaration(key: Key @unchecked, declaration: Facility @unchecked) =>
      storage
        .addFacility(origin, key, declaration)
        .flatMap(m => handleStorageResponse(key, m))

    case ConsensusPeerDeclaration(key: Key @unchecked, declaration: Proposal @unchecked) =>
      storage
        .addProposal(origin, key, declaration)
        .flatMap(m => handleStorageResponse(key, m))

    case ConsensusPeerDeclaration(key: Key @unchecked, declaration: MajoritySignature @unchecked) =>
      storage
        .addSignature(origin, key, declaration)
        .flatMap(m => handleStorageResponse(key, m))

    case ConsensusPeerDeclaration(key: Key @unchecked, declaration: BinarySignature @unchecked) =>
      storage
        .addBinarySignature(origin, key, declaration)
        .flatMap(m => handleStorageResponse(key, m))

    case ConsensusPeerDeclarationAck(key: Key @unchecked, kind: Kind @unchecked, ack) =>
      storage
        .addPeerDeclarationAck(origin, key, kind, ack)
        .flatMap(m => handleStorageResponse(key, m))

    case ConsensusWithdrawPeerDeclaration(key: Key @unchecked, kind: Kind @unchecked) =>
      storage
        .addWithdrawPeerDeclaration(origin, key, kind)
        .flatMap(m => handleStorageResponse(key, m))

    case ConsensusEvent(eventVal: Event @unchecked) =>
      if (fns.triggerPredicate(eventVal))
        storage.addTriggerEvent(origin, (ordinal, eventVal)) *> enqueueEventTrigger
      else
        storage.addEvent(origin, (ordinal, eventVal))

    case ConsensusArtifact(key: Key @unchecked, artifact: Artifact @unchecked) =>
      HasherSelector[F].withCurrent { implicit h =>
        storage.addArtifact(key, artifact)
      }.flatMap(m => handleStorageResponse(key, m))

    case other =>
      log.warn(s"Unknown peer rumor content: $other")
  }

  private def applyCommonRumor(content: Any): F[Unit] =
    content match {
      case ConsensusArtifact(key: Key @unchecked, artifact: Artifact @unchecked) =>
        HasherSelector[F].withCurrent { implicit h =>
          storage.addArtifact(key, artifact)
        }.flatMap(m => handleStorageResponse(key, m))

      case ConsensusEvent(eventVal: Event @unchecked) =>
        if (fns.triggerPredicate(eventVal))
          // CommonRumor has no origin or ordinal -> treat as pure internal event
          ctx.queue.offer(FacilitateByEvent)
        else
          Async[F].unit

      case other =>
        log.warn(s"Unknown common rumor content: $other")
    }

  // ---------------------------------------------------------------------------
  // STORAGE / STATE UPDATES
  // ---------------------------------------------------------------------------

  private def handleStorageResponse(key: Key, maybe: Option[_]): F[Unit] =
    maybe match {
      case None => Async[F].unit
      case Some(_) => ctx.queue.offer(CheckUpdate(key))
    }

  private def doCheckUpdate(key: Key): F[Unit] =
    storage.getResources(key).flatMap { resources =>
      ctx.updater.tryUpdateConsensus(key, resources).flatMap {
        case None => Async[F].unit

        case Some((_, newState)) =>
          ctx.advancer.getConsensusOutcome(newState) match {

            case Some((prevKey, newOutcome)) =>
              storage
                .tryUpdateLastConsensusOutcomeWithCleanup(prevKey, newOutcome)
                .void

            case None =>
              Async[F].unit
          }
      }
    }

  private def enqueueEventTrigger: F[Unit] =
    ctx.queue.offer(FacilitateByEvent)

  private def registerPeer(peer: Peer): F[Unit] =
    storage.getLastConsensusOutcome.flatMap {
      case None =>
        Async[F].unit

      case Some(outcome) =>
        val key = _key.get(outcome)
        storage.registerPeer(peer.id, key).void.handleError(_ => ())
    }

  private def handleRoundCompleted: F[Unit] =
    for {
      next <- pending.pullNext
      _ <- next match {
        case Some(TriggerPriority.Time) =>
          ctx.queue.offer(StartRound(TimeTrigger))

        case Some(TriggerPriority.Event) =>
          ctx.queue.offer(StartRound(EventTrigger))

        case None =>
          Async[F].unit
      }
      _ <- isRunning.set(false)
    } yield ()

  private def fetchOutcomeFromCluster(
    key         : Key,
    lastArtifact: Signed[Artifact],
    lastContext : Ctx
  ): F[Option[Outcome]] = {

    def selectRandomPeer: F[Peer] =
      ctx.clusterStorage.getResponsivePeers
        .map(_.filter(_.state == NodeState.Ready))
        .flatMap(peers => Random[F].elementOf(peers.toSeq))

    def fetchConsensusOutcome(peer: Peer): F[Option[Outcome]] =
      ctx.consensusClient
        .getSpecificConsensusOutcome(GetConsensusOutcomeRequest(key))
        .run(peer)

    def wasSuccessful(maybe: Option[Outcome]): F[Boolean] =
      maybe.exists { outcome =>
        _key.get(outcome) == key &&
          _artifact.get(outcome) == lastArtifact &&
          _context.get(outcome) == lastContext
      }.pure[F]

    val retryPolicy =
      limitRetries(10).join(constantDelay(3.seconds))

    (selectRandomPeer >>= fetchConsensusOutcome)
      .retryingOnFailuresAndAllErrors(
        wasSuccessful = wasSuccessful,
        policy = retryPolicy,
        onFailure = (_, _) => log.info(s"[DownloadInit] Retrying outcome observation for key=$key"),
        onError = (err, _) => log.error(err)(s"[DownloadInit] Error fetching outcome")
      )
  }

  private def handleInitializeFromDownload(
    key         : Key,
    lastArtifact: Signed[Artifact],
    lastContext : Ctx
  ): F[Unit] =
    for {
      _ <- log.info(s"[DownloadInit] Initializing consensus at key=$key")
      maybeOutcome <- fetchOutcomeFromCluster(key, lastArtifact, lastContext)

      outcome <- maybeOutcome.liftTo[F](
        new Throwable(s"[DownloadInit] Could not observe consensus outcome for key=$key")
      )

      _ <- storage
        .trySetInitialConsensusOutcome(outcome)
        .ifM(
          ctx.nodeStorage.tryModifyState(NodeState.Observing, NodeState.WaitingForReady) >>
            ctx.queue.offer(StartRound(_trigger.get(outcome))), // kick the engine
          new Throwable(s"[DownloadInit] Failed to initialize consensus storage").raiseError[F, Unit]
        )
    } yield ()

  private def handleInitializeFromRollback(key: Key, outcome: Outcome): F[Unit] =
    for {
      _ <- log.info(s"[RollbackInit] Initializing consensus after rollback at key=$key")
      _ <- storage.trySetInitialConsensusOutcome(outcome)
      _ <- isRunning.set(false)
      _ <- ctx.queue.offer(StartRound(TimeTrigger))
    } yield ()

  private def handleWithdraw: F[Unit] =
    for {
      _ <- storage.clearObservationKey
      _ <- ctx.nodeStorage.tryModifyState(NodeState.Observing, NodeState.Ready)
      _ <- isRunning.set(false)
    } yield ()
}
