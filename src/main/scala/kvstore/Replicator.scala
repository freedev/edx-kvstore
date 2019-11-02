package kvstore

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import akka.event.Logging
import akka.pattern.{Patterns, RetrySupport}
import akka.util.Timeout
import kvstore.Persistence.Persist
import kvstore.Replica.{Insert, OperationAck, OperationReply, Remove, SendMessage}
import akka.pattern.{CircuitBreaker, ask, pipe}

import scala.concurrent.Await
import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._


  val log = Logging(context.system, this)
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  var childrenActors = Set.empty[ActorRef]

  override def preStart(): Unit = {
   // log.info("Replicator - preStart " + self)
    //    sendMessage()
  }

  var _seqCounter = 0L
  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }
  
  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case r: PoisonPill => {
// log.info("Replica - secondary received message: PoisonPill")
      childrenActors foreach (child => child ! PoisonPill)
      context.stop(self)
    }
    case r:SnapshotAck => {
// log.info("Replicator received message SnapshotAck - " + replica)
      context.parent ! Replicated(r.key, r.seq)

      //      val v = acks.get(r.seq)
//      if (v.isEmpty) {
//        log.info("Replicator acks empty for " + r.seq)
//      }
//      v.foreach(ref => {
//        log.info("Replicator send message Replicated to " + ref._1)
//        ref._1 ! Replicated(r.key, r.seq)
//      })
    }
    case r:Replicate => {
// log.info("Replicator received message Replicate")

      val msg = Snapshot(r.key, r.valueOption, r.id)

      acks = acks + ((r.id, (sender, r)))

      childrenActors = childrenActors + context.actorOf(Props(classOf[ReplicatorChild], sender(), replica, msg))
//      replicatorChild ! SendMessage(r.key, r.id)

    }
    case m => {
// log.info("Replicator received unhandled message " + m + " from " + sender())
    }
  }

}
