package kvstore

import java.util.concurrent.Callable

import akka.actor.{Actor, ActorRef, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated}
import akka.event.Logging
import kvstore.Arbiter.{JoinedPrimary, _}
import akka.pattern.{CircuitBreaker, RetrySupport, ask, pipe}

import scala.concurrent.duration._
import akka.util.Timeout

import scala.concurrent.Future

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }

  case class StoreValue(value: Option[String], id: Long)

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
  import akka.pattern.Patterns
  import scala.concurrent.Await

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
    case r:Persisted => {
      log.info("leader received message: Persisted")
    }
    case r:Replicas => {
      log.info("leader received message: Replicas")
      r.replicas foreach( curRep => {
        // Only replicas not contained into secondaries map should be updated
        if (! secondaries.contains(curRep)) {

          log.info("leader start replication to new replica " + curRep)
          val replicator = context.actorOf(Replicator.props(curRep))
          replicators = replicators + replicator
          secondaries = secondaries + ((curRep, replicator))

          kv.foreach( element => {
            replicator.tell(Replicate(element._1, element._2.value, element._2.id), this.self)
          })
        }
      } )
      // r.replicas foreach
    }
    case r:Get => {
      if (kv.contains(r.key)) {
        context.sender() ! GetResult(r.key, kv.get(r.key).get.value, r.id)
      } else {
        context.sender() ! GetResult(r.key, None, r.id)
      }
    }
    case r:Insert => {
      log.info("Received Insert")
      val v = kv.get(r.key)
      if (v.isDefined) {
        log.info("key is defined " + r.key)
        if (v.get.id < r.id) {
          saveInsert(r, context.sender())
        } else {
          context.sender() ! OperationFailed(r.id)
        }
      } else {
        log.info("key is NOT defined " + r.key)
        saveInsert(r, context.sender())
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

  private def saveInsert(r: Insert, sender: ActorRef) = {
    val storeValue = StoreValue(Some(r.value), r.id)
    val persistence = context.actorOf(persistenceProps)
    val p = Persist(r.key, Some(r.value), r.id)
    implicit val timeout = Timeout(800 milliseconds)
    implicit val scheduler=context.system.scheduler

//    val cb100 = CircuitBreaker(context.system.scheduler, maxFailures = 8, callTimeout = 100 millisecond, resetTimeout = 100 milliseconds)
//    val future = cb100.withCircuitBreaker(persistence ? p)
    val future = RetrySupport.retry(() => {
      log.info("sent message Snapshot to replica")
      Patterns.ask(persistence, p, 100 millisecond)
    }, 8, 100 millisecond)

    future onFailure {
      case _ => {
        sender ! OperationFailed(p.id)
      }
    }
    future onSuccess {
      case p: Persisted => {
        log.info("Received Persisted - Send to OperationAck")
        kv = kv + ((p.key, storeValue))
        sender ! OperationAck(p.id)
        secondaries.values.foreach(secondaryReplicator => {
          log.info("sending snapshot to all replicators " + secondaryReplicator)
          secondaryReplicator.tell(Snapshot(r.key, Some(r.value), r.id), secondaryReplicator)
        })
      }
      case _ => {
        log.info("saveInsert - Replica received something from persistence")
      }
    }
  }

  private def saveSnapshot(r: Snapshot, replicator: ActorRef) = {

    implicit val timeout = Timeout(3 second)
    kv = kv + ((r.key, StoreValue(r.valueOption, r.seq)))

    val p = Persist(r.key, r.valueOption, r.seq)
    val persistence = context.actorOf(persistenceProps)

    log.info("withCircuitBreaker... start")
    val cb100 = CircuitBreaker(context.system.scheduler, maxFailures = 6, callTimeout = 900 millisecond, resetTimeout = 450 millisecond )
    val future = cb100.withCircuitBreaker(persistence ? p)
    log.info("withCircuitBreaker... end")
    future onSuccess {
      case p1: Persisted => {
        log.info("Replica received Persisted - send back to sender SnapshotAck(" + p1.key + "," + p1.id + ")")
        replicator.tell(SnapshotAck(p1.key, p1.id), this.self)
      }
      case _ => {
        log.info("saveSnapshot - Replica received something from persistence")
      }
    }
//    future.onComplete(t => {
//      log.info("Replica onComplete - (" + t + ")")
//    })
//    implicit val scheduler=context.system.scheduler
//    val future = RetrySupport.retry(() => {
//      log.info("Retry to Persist")
//      Patterns.ask(persistence, p, 100 millisecond)
//    }, 30, 100 millisecond)
//    // val result = Await.result(future, 3 second)
//    future onSuccess {
//      case p1: Persisted => {
//        log.info("Replica received Persisted - send back to sender SnapshotAck(" + p1.key + "," + p1.id + ")")
//        replicator.tell(SnapshotAck(p1.key, p1.id), this.self)
//      }
//      case _ => {
//        log.info("saveSnapshot - Replica received something from persistence")
//      }
//    }
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case r:Persisted => {
      log.info("replica received message: Persisted")
    }
    case r:Snapshot => {
      log.info("Replica received message Snapshot")
      val v = kv.get(r.key)
      val kvId = v.getOrElse(StoreValue(null, 0L))
      if (v.isEmpty && r.valueOption.isEmpty && r.seq == kvId.id) {
        log.info("Before saveSnapshot 1")
        saveSnapshot(r, context.sender())
      } else if (v.isDefined) {
        if (r.seq > kvId.id) {
          log.info("Before saveSnapshot 2")
          saveSnapshot(r, context.sender())
        } else {
          context.sender() ! SnapshotAck(r.key, r.seq)
          log.info("Replica sent message SnapshotAck")
        }
      } else {
          if (r.seq == 0) {
            log.info("Before saveSnapshot 3")
            saveSnapshot(r, context.sender())
          }
      }
    }
    case r:Get => {
      val res = kv.get(r.key).getOrElse(StoreValue(None, 0L))
      context.sender() ! GetResult(r.key, res.value, r.id)
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

