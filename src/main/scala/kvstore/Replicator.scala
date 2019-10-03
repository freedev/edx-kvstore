package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
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
  import context.dispatcher


  val log = Logging(context.system, this)
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  override def preStart(): Unit = {
    log.info("Replicator - preStart " + self)
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
    case r:SnapshotAck => {
      log.info("Replicator received message SnapshotAck - " + replica)

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
      log.info("Replicator received message Replicate")

      val msg = Snapshot(r.key, r.valueOption, r.id)
      val leader = sender()

      acks = acks + ((r.id, (sender, r)))
      implicit val scheduler=context.system.scheduler

      val replicatorChild = context.actorOf(Props(classOf[ReplicatorChild], self, leader, replica, msg))
      replicatorChild ! SendMessage(r.key, r.id)

    }
    case m => {
      log.info("Replicator received unhandled message " + m + " from " + sender())
    }
  }

}
