package io.constellationnetwork.node.shared.infrastructure.consensus.engine

import io.constellationnetwork.node.shared.infrastructure.consensus.message._
import io.constellationnetwork.node.shared.infrastructure.consensus.trigger.ConsensusTrigger
import io.constellationnetwork.schema.gossip.{CommonRumor, PeerRumor, RumorRaw}
import io.constellationnetwork.schema.peer.Peer

sealed trait ConsensusCommand

object ConsensusCommand {

  // --- existing ---
  case object TimeTick extends ConsensusCommand
  case object FacilitateByEvent extends ConsensusCommand
  final case class RumorReceived[A](rumor: Either[PeerRumor[A], CommonRumor[A]]) extends ConsensusCommand

  final case class CheckUpdate[K](key: K) extends ConsensusCommand
  final case class PeerObserved(peer: Peer) extends ConsensusCommand
  final case class InternalScheduled(cmd: ConsensusCommand) extends ConsensusCommand
  final case class StartRound(trigger: ConsensusTrigger) extends ConsensusCommand
  final case class IgnoreUnexpectedRumor(raw: RumorRaw) extends ConsensusCommand
  case object RoundCompleted extends ConsensusCommand

  // --- NEW: required to replace old API ---
  final case class InitializeFromDownload[K, A, C](
    key: K,
    lastArtifact: A,
    lastContext: C
  ) extends ConsensusCommand

  final case class InitializeFromRollback[K, O](
    key: K,
    initialOutcome: O
  ) extends ConsensusCommand

  case object WithdrawFromConsensus extends ConsensusCommand
}
