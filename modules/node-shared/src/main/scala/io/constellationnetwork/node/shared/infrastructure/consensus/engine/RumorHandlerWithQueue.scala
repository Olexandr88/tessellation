package io.constellationnetwork.node.shared.infrastructure.consensus.engine

import cats.Applicative
import cats.data.{Kleisli, OptionT}
import cats.effect.Async
import cats.effect.std.Queue
import cats.syntax.all._

import scala.reflect.runtime.universe.TypeTag
import io.circe.Decoder
import io.constellationnetwork.schema.gossip._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.syntax.boolean._
import io.constellationnetwork.node.shared.infrastructure.consensus.engine.ConsensusCommand._
import io.constellationnetwork.node.shared.infrastructure.gossip.{ExcludeSelfOrigin, IncludeSelfOrigin, OriginPolicy, RumorHandler}

/**
 * FS2-based version of the classic RumorHandler.
 *
 *  - Filters by content type (ContentType[A])
 *  - Decodes only matching rumors
 *  - Automatically enqueues ConsensusCommand.RumorReceived(...)
 */
object RumorHandlerWithQueue {

  /** Handle PEER rumor of type A and enqueue ConsensusCommand */
  def peer[F[_]: Async, A: TypeTag: Decoder](
    queue: Queue[F, ConsensusCommand],
    selfOriginPolicy: OriginPolicy = IncludeSelfOrigin
  ): RumorHandler[F] = {

    val handlerContentType = ContentType.of[A]

    val pf = new PartialFunction[(RumorRaw, PeerId), F[Unit]] {

      def isDefinedAt(v: (RumorRaw, PeerId)): Boolean = v match {
        case (rumor, selfId) =>
          rumor.isInstanceOf[PeerRumorRaw] &&
            rumor.contentType === handlerContentType &&
            ((rumor.asInstanceOf[PeerRumorRaw].origin === selfId) ==> (selfOriginPolicy =!= ExcludeSelfOrigin))
      }

      def apply(v: (RumorRaw, PeerId)): F[Unit] = v match {
        case (raw: PeerRumorRaw, selfId) =>
          for {
            decoded <- raw.content.as[A].liftTo[F]
            pr       = PeerRumor(raw.origin, raw.ordinal, decoded)
            cmd      = RumorReceived(Left(pr))
            _       <- queue.offer(cmd)
          } yield ()

        case (u, _) =>
          UnexpectedRumorClass(u).raiseError[F, Unit]
      }
    }

    pfToKleisli(pf)
  }


  /** Handle COMMON rumor of type A and enqueue ConsensusCommand */
  def common[F[_]: Async, A: TypeTag: Decoder](
    queue: Queue[F, ConsensusCommand]
  ): RumorHandler[F] = {

    val handlerContentType = ContentType.of[A]

    val pf = new PartialFunction[(RumorRaw, PeerId), F[Unit]] {

      def isDefinedAt(v: (RumorRaw, PeerId)): Boolean = v match {
        case (rumor, _) =>
          rumor.isInstanceOf[CommonRumorRaw] &&
            rumor.contentType === handlerContentType
      }

      def apply(v: (RumorRaw, PeerId)): F[Unit] = v match {
        case (raw: CommonRumorRaw, _) =>
          for {
            decoded <- raw.content.as[A].liftTo[F]
            cr       = CommonRumor(decoded)
            cmd      = RumorReceived(Right(cr))
            _       <- queue.offer(cmd)
          } yield ()

        case (u, _) =>
          UnexpectedRumorClass(u).raiseError[F, Unit]
      }
    }

    pfToKleisli(pf)
  }


  /** Convert partial function → Kleisli */
  private def pfToKleisli[F[_]: Applicative](
    pf: PartialFunction[(RumorRaw, PeerId), F[Unit]]
  ): RumorHandler[F] =
    Kleisli { case input =>
      OptionT( pf.lift(input).sequence )
    }
}