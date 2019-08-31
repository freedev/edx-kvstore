package kvstore

import akka.actor.{Actor, ActorRef, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated}
import akka.event.Logging
import kvstore.Arbiter.{JoinedPrimary, _}
import akka.pattern.{ask, pipe}

import scala.concurrent.duration._
import akka.util.Timeout

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }

  case class StoreValue(value: String, id: Long)

  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  val log = Logging(context.system, this)

  arbiter.tell(Join, this.self)

  log.info("replica tells to the arbiter: Join")

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, StoreValue]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case r:Get => {
      if (kv.contains(r.key)) {
        context.sender() ! GetResult(r.key, Some(kv.get(r.key).get.value), r.id)
      } else {
        context.sender() ! GetResult(r.key, None, r.id)
      }
    }
    case r:Insert => {
      val v = kv.get(r.key)
      if (v.isDefined) {
        if (v.get.id < r.id) {
          kv + ((r.key, StoreValue(r.value, r.id)))
          context.sender() ! OperationAck(r.id)
        } else {
          context.sender() ! OperationFailed(r.id)
        }
      } else {
        kv = kv + ( (r.key, StoreValue(r.value, r.id)) )
        context.sender() ! OperationAck(r.id)
      }
    }
    case r:Remove => {
      val v = kv.get(r.key)
      if (v.isDefined) {
        kv  = kv - (r.key)
        context.sender() ! OperationAck(r.id)
      } else {
        context.sender() ! OperationFailed(r.id)
      }
    }
    case _ =>
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case r:Get => {
      val res = kv.get(r.key).map(v => v.value).orElse(None)
      context.sender() ! GetResult(r.key, res, r.id)
    }
    case r:Insert => {
      context.sender() ! OperationFailed(r.id)
    }
    case r:Remove => {
      context.sender() ! OperationFailed(r.id)
    }
    case _ =>
  }

}

