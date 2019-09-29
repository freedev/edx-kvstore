package kvstore

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated}
import akka.event.Logging
import kvstore.Arbiter.{JoinedPrimary, _}
import akka.pattern.{AskTimeoutException, CircuitBreaker, RetrySupport, ask, pipe}

import scala.concurrent.duration._
import akka.util.Timeout

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }

  case class StoreValue(value: Option[String], id: Long)

  case class SendMessage(key: String, id: Long) extends Operation
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

  val persistence = context.actorOf(persistenceProps)

  arbiter.tell(Join, this.self)

  log.info("replica tells to the arbiter: Join")

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, StoreValue]
  var replicatedAck = Map.empty[String, Int]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  def checkSecondaries(size: Int, r: PersistAck, self: ActorRef, tentatives: Int): Unit = {
    import scala.language.postfixOps
    if (tentatives < 20) {
      val num = replicatedAck.get(r.key)
      if (num.isEmpty) {
        r.sender.tell(OperationAck(r.id), self)
      } else {
        num.foreach(v => {
          log.info("Replica - checkSecondaries replicatedAck " + r.key + "=" + v)
          if (v > 0) {
            context.system.scheduler.scheduleOnce(100 milliseconds)({
              checkSecondaries(size, r, self, tentatives + 1)
            })
          } else {
            r.sender.tell(OperationAck(r.id), self)
          }
        })
      }
    } else {
      r.sender.tell(OperationFailed(r.id), self)
    }
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case r:PersistAck => {
      log.info("Replica - leader received message: PersistAck")
      kv = kv + ((r.key, StoreValue(r.valueOption, r.id)))
      replicatedAck = replicatedAck + ((r.key, secondaries.size))
      secondaries.values.foreach(secondaryReplicator => {
        log.info("Replica - sending Replicate to all replicators " + secondaryReplicator)
        secondaryReplicator.tell(Replicate(r.key, r.valueOption, r.id), secondaryReplicator)
      })
      checkSecondaries(secondaries.size, r, this.self, 0)
    }
    case r:Replicas => {
      log.info("Replica - leader received message: Replicas")
      r.replicas foreach( curRep => {
        // Only replicas not contained into secondaries map should be updated
        if (! secondaries.contains(curRep)) {

          log.info("Replica - leader start replication to new replica " + curRep)
          val replicator = context.actorOf(Replicator.props(curRep))
          replicators = replicators + replicator
          secondaries = secondaries + ((curRep, replicator))

          kv.foreach( element => {
            log.info("Replica - leader start replication to new replicator " + replicator)
            replicator.tell(Replicate(element._1, element._2.value, element._2.id), replicator)
          })
        }
      } )
      // r.replicas foreach
    }
    case r:Get => {
      log.info("Replica - received Get " + r.key)
      if (kv.contains(r.key)) {
        log.info("Replica - " + r.key + " Found ")
        sender() ! GetResult(r.key, kv.get(r.key).get.value, r.id)
      } else {
        log.info("Replica - " + r.key + " NOT Found ")
        sender() ! GetResult(r.key, None, r.id)
      }
    }
    case r:Insert => {
      log.info("Replica - received Insert")
      val v = kv.get(r.key)
      if (v.isDefined) {
        log.info("Replica - key is defined " + r.key)
        if (v.get.id < r.id) {
          log.info("Replica - key " + r.key + " request id is higher than saved. Saving...")
          val p = Persist(r.key, Some(r.value), r.id)
          savePersist(p, sender())
        } else {
          log.info("Replica - key " + r.key + " request id is lower than saved. NOT Saving. Sending back OperationFailed to the client.")
          sender() ! OperationFailed(r.id)
        }
      } else {
        log.info("Replica - key " + r.key + " is not Defined. Saving...")
        val p = Persist(r.key, Some(r.value), r.id)
        savePersist(p, sender())
      }
    }
    case r:Remove => {
      val v = kv.get(r.key)
      if (v.isDefined) {
        kv  = kv - (r.key)
        log.info("Replica - key " + r.key + " removed. Sending OperationAck to the client ")
        val p = Persist(r.key, None, r.id)
        savePersist(p, sender())
      } else {
        log.info("Replica - key " + r.key + " NOT removed. Sending OperationFailed to the client ")
        sender() ! OperationFailed(r.id)
      }
    }
    case m => {
      log.error("Replica - leader received unhandled message " + m)
    }
  }

  private def savePersist(p: Persist, sender: ActorRef) = {
    implicit val timeout = Timeout(800.milliseconds)
    implicit val scheduler=context.system.scheduler

    log.info("Replica - creating actorChildB " + p.id)

    val actorChildB = context.actorOf(Props(classOf[ReplicaChild], self, sender, persistence, p))
    actorChildB ! SendMessage(p.key, p.id)

  }

  private def saveSnapshot(r: Snapshot, replicator: ActorRef):Unit = {
    log.info("Replica - saveSnapshot - start")

    kv = kv + ((r.key, StoreValue(r.valueOption, r.seq)))

    val p = Persist(r.key, r.valueOption, r.seq)
    implicit val timeout = Timeout(800.milliseconds)
    implicit val scheduler=context.system.scheduler

    log.info("Replica - creating actorChildB " + r.seq)

    val actorChildB = context.actorOf(Props(classOf[ReplicaChild], self, replicator, persistence, p))
    actorChildB ! SendMessage(r.key, r.seq)

    }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case r:Replicated => {
      log.info("Replica - secondary received message: Replicated")
      val num = replicatedAck.get(r.key)
      num.foreach(v => {
        replicatedAck = replicatedAck + ((r.key, v - 1))
      })
    }
    case r:PersistAck => {
      log.info("Replica - secondary received message: PersistAck")
      r.sender.tell(SnapshotAck(r.key, r.id), this.self)
    }
    case r:Snapshot => {
      log.info("Replica received message Snapshot")
      val v = kv.get(r.key)
      val kvId = v.getOrElse(StoreValue(null, 0L))
      if (v.isEmpty && r.valueOption.isEmpty && r.seq == kvId.id) {
        log.info("Before saveSnapshot 1")
        saveSnapshot(r, sender())
      } else if (v.isDefined) {
        if (r.seq > kvId.id) {
          log.info("Before saveSnapshot 2")
          saveSnapshot(r, sender())
        } else {
          sender() ! SnapshotAck(r.key, r.seq)
          log.info("Replica sent message SnapshotAck")
        }
      } else {
          if (r.seq == 0) {
            log.info("Replica - Before saveSnapshot 3")
            saveSnapshot(r, sender())
          }
      }
    }
    case r:Get => {
      val res = kv.get(r.key).getOrElse(StoreValue(None, 0L))
      sender() ! GetResult(r.key, res.value, r.id)
    }
    case r:Insert => {
      sender() ! OperationFailed(r.id)
    }
    case r:Remove => {
      sender() ! OperationFailed(r.id)
    }
    case m => {
      log.error("Replica - secondary received unhandled message " + m)
    }
  }

}

