package kvstore

import akka.actor.{Actor, ActorRef}
import akka.event.Logging

import scala.collection.immutable

object Arbiter {
  case object Join
  case object Joined
  case object JoinedPrimary
  case object JoinedSecondary


  /**
   * This message contains all replicas currently known to the arbiter, including the primary.
   */
  case class Replicas(replicas: Set[ActorRef])
  case class GetLeader()
  case class GetLeaderAck(curLeader:Option[ActorRef])
}

class Arbiter extends Actor {
  import Arbiter._
  var leader: Option[ActorRef] = None
  var replicas = Set.empty[ActorRef]

  val log = Logging(context.system, this)

  def receive = {
    case GetLeader => {
      sender() ! GetLeaderAck(leader)
    }
    case Join =>
      if (leader.isEmpty) {
        log.info("leader is empty")
        leader = Some(sender)
        replicas += sender
        sender ! JoinedPrimary
      } else {
        log.info("leader is not empty")
        replicas += sender
        sender ! JoinedSecondary
      }
      leader foreach (_ ! Replicas(replicas))
  }

}
