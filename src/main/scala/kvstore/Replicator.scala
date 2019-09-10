package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import akka.event.Logging
import akka.pattern.{CircuitBreaker, Patterns, RetrySupport}
import akka.util.Timeout
import kvstore.Persistence.Persist
import kvstore.Replica.{Insert, OperationAck, OperationReply, Remove}
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
  
  var _seqCounter = 0L
  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }
  
  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case r:Replicate => {
      log.info("Replicator received message Replicate")

      val msg = Snapshot(r.key, r.valueOption, r.id)
      val sender = context.sender()
      implicit val scheduler=context.system.scheduler

//      implicit val timeout = Timeout(3 second)
//      val cb100 = CircuitBreaker(context.system.scheduler, maxFailures = 50, callTimeout = 3 seconds, resetTimeout = 100 milliseconds )
//      val future = cb100.withCircuitBreaker(replica ? msg)

      val future = RetrySupport.retry(() => {
        log.info("sent message Snapshot to replica")
        Patterns.ask(replica, msg, 100 millisecond)
      }, 50, 100 millisecond)

      future onSuccess {
        case s:SnapshotAck => {
          log.info("received message SnapshotAck")
          sender ! Replicated(s.key, s.seq)
        }
      }

    }
    case _ =>
  }

}
