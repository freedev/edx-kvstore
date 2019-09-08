package kvstore

import java.util.concurrent.Callable

import akka.actor.{Actor, ActorRef, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated}
import akka.event.Logging
import kvstore.Arbiter.{JoinedPrimary, _}
import akka.pattern.{RetrySupport, ask, pipe}

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

  val persistence = context.actorOf(persistenceProps)

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
    case r:Replicas => {
      log.info("leader received message: Replicas")
      r.replicas foreach( curRep => {
        // Only replicas not contained into secondaries map should be updated
        if (! secondaries.contains(curRep)) {
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
      val v = kv.get(r.key)
      if (v.isDefined) {
        if (v.get.id < r.id) {
          saveInsert(r)
        } else {
          context.sender() ! OperationFailed(r.id)
        }
      } else {
        saveInsert(r)
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

  private def saveInsert(r: Insert) = {
//    val timeout = new Timeout(Duration.create(1, "seconds"))
    implicit val scheduler=context.system.scheduler
    val future = RetrySupport.retry(() => {
      Patterns.ask(persistence, Persist(r.key, Some(r.value), r.id), 1 second)
    }, 1, 1 second)
    val result = Await.result(future, 1 second)

    result match {
      case _: Persisted => {
        kv = kv + ((r.key, StoreValue(Some(r.value), r.id)))
        context.sender() ! OperationAck(r.id)
        secondaries.keys.foreach(secondaryReplica => {
          secondaryReplica
        })
      }
    }
  }

  private def saveSnapshot(r: Snapshot, id: Long) = {
    //    val timeout = new Timeout(Duration.create(1, "seconds"))
    implicit val scheduler=context.system.scheduler
    val future = RetrySupport.retry(() => {
      Patterns.ask(persistence, Persist(r.key, r.valueOption, r.seq), 100 millisecond)
    }, 1, 1 second)
    val result = Await.result(future, 100 millisecond)

    result match {
      case _: Persisted => {
        kv = kv + ((r.key, StoreValue(r.valueOption, r.seq)))
        context.sender() ! SnapshotAck(r.key, r.seq)
      }
    }
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case r:Snapshot => {
      val v = kv.get(r.key)
      var kvId = v.getOrElse(StoreValue(null, 0L))
      if (v.isEmpty && r.valueOption.isEmpty && r.seq == kvId.id) {
        saveSnapshot(r, kvId.id)
      } else if (v.isDefined) {
        if (r.seq > kvId.id) {
          saveSnapshot(r, kvId.id)
        } else {
          context.sender() ! SnapshotAck(r.key, r.seq)
        }
      } else {
        if (r.valueOption.isDefined) {
          if (r.seq == 0) {
            saveSnapshot(r, kvId.id)
          }
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

