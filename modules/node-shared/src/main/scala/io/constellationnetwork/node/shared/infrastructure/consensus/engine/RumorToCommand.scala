package io.constellationnetwork.node.shared.infrastructure.consensus.engine

import cats.syntax.all._

import io.constellationnetwork.node.shared.infrastructure.consensus.engine.ConsensusCommand._
import io.constellationnetwork.schema.gossip.{CommonRumor, PeerRumor}

/** RumorToCommand: Converts gossip messages into ConsensusEngine commands.
  */
object RumorToCommand {
  def apply[A](rumor: A): ConsensusCommand =
    rumor match {
      case pr @ PeerRumor(_, _, _) =>
        RumorReceived(Left(pr))

      case cr @ CommonRumor(_) =>
        RumorReceived(Right(cr))

      case other =>
        RumorReceived(Right(CommonRumor(other)))
    }
}
