package kvstore

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import akka.event.Logging
import kvstore.Arbiter.{JoinedPrimary, _}
import kvstore.MassiveReplicatorChild.{MassiveReplicatorDead, RemovedReplica}
import kvstore.ReplicaChild.ReplicaChildDead

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

  import Persistence._
  import Replica._
  import Replicator._

  val log = Logging(context.system, this)

  val persistence = context.actorOf(persistenceProps)

  arbiter.tell(Join, this.self)

  log.info("replica tells to the arbiter: Join")

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, StoreValue]
  var replicatedAck = Map.empty[Long, Int]
  var msgCounterMap = Map.empty[Long, Long]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var childrenActors = Set.empty[ActorRef]

  var minId = -1L

  override def preStart(): Unit = {
    log.info("Replica - preStart " + self)
    //    sendMessage()
  }

  override def postStop(): Unit = {
    super.postStop()
    log.info("Replica - postStop ")
  }

  def receive = {
    case JoinedPrimary => {
      context.become(leader)
    }
    case JoinedSecondary => {
      context.become(replica)
    }
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case r: Replicated => {
      log.info("Replica - leader received message: Replicated " + replicatedAck(r.id))
      val ackSize = replicatedAck(r.id) - 1
      if (ackSize == 0) {
        replicatedAck = replicatedAck - (r.id)
      } else {

      }
    }
    case r: ReplicaChildDead => {
      log.info("Replica - leader received message: ReplicaChildDead ")
      childrenActors = childrenActors - sender()
    }
    case r: MassiveReplicatorDead => {
      log.info("Replica - leader received message: MassiveReplicatorDead ")
      childrenActors = childrenActors - sender()
    }
    case r: PersistAck => {
      log.info("Replica - leader received message: PersistAck " + r)
      replicatedAck = replicatedAck + ((r.id, secondaries.size))
      if (secondaries.isEmpty) {
        val optAck = OperationAck(r.id)
        log.info("Replica - leader send message: OperationAck " + optAck + " back to " + r.sender)
        r.sender.tell(optAck, self)
      } else {
        log.info("Replica - leader received message: PersistAck starting MassiveReplicatorChild")
        val replicate = Replicate(r.key, r.valueOption, r.id)
        childrenActors = childrenActors + context.actorOf(Props(classOf[MassiveReplicatorChild], secondaries, r.sender, replicate))
      }
    }
    case r: Replicas => {
      log.info("Replica - leader received message: Replicas " + secondaries.size + " " + r.replicas.size)

      var deadReplicas = Set.empty[ActorRef]

      secondaries foreach (entry => {
        if (!r.replicas.contains(entry._1)) {
          log.info("Replica - sending poisonPill to Replicas " + entry._1)
          log.info("Replica - sending poisonPill to Replicator " + entry._2)
          entry._1 ! PoisonPill
          entry._2 ! PoisonPill
          deadReplicas = deadReplicas + entry._1
          replicators = replicators - entry._2
        }
      })
      deadReplicas foreach (deadReplica => {
        secondaries = secondaries - deadReplica
        childrenActors.foreach(child => {
          log.info("Replica - sending RemovedReplica to childActor " + child)
          child ! RemovedReplica(deadReplica)
        })
      })
      r.replicas foreach (curRep => {
        // Only replicas not contained into secondaries map should be updated
        if (!curRep.equals(self)) {
          if (!secondaries.contains(curRep)) {
            log.info("Replica - leader start replication to new replica " + curRep + " self " + self)
            val replicator = context.actorOf(Replicator.props(curRep))
            replicators = replicators + replicator
            secondaries = secondaries + ((curRep, replicator))

            kv.foreach(element => {
              log.info("Replica - leader start replication to new replicator " + replicator)
              replicator.tell(Replicate(element._1, element._2.value, element._2.id), self)
            })
          }
        }
      })
      // r.replicas foreach
    }
    case r: Get => {
      log.info("Replica - received Get " + r.key)
      if (kv.contains(r.key)) {
        log.info("Replica - " + r.key + " Found ")
        sender() ! GetResult(r.key, kv.get(r.key).get.value, r.id)
      } else {
        log.info("Replica - " + r.key + " NOT Found ")
        sender() ! GetResult(r.key, None, r.id)
      }
    }
    case r: Insert => {
      log.info("Replica - leader - received Insert " + r + " from " + sender())
      val v = kv.get(r.key)
      if (v.isDefined) {
        log.info("Replica - key is defined " + r.key)
        if (v.get.id <= r.id) {
          log.info("Replica - key " + r.key + " request id is higher than saved. Saving...")
          val p = Persist(r.key, Some(r.value), r.id)
          savePersist(p, sender())
        } else {
          log.info("Replica - key " + r.key + " request id is lower than saved. NOT Saving. Sending back OperationFailed to the client.")
          sender() ! OperationFailed(r.id)
        }
      } else {
        minId = r.id
        log.info("Replica - key " + r.key + " is not Defined. Saving...")
        val p = Persist(r.key, Some(r.value), r.id)
        savePersist(p, sender())
      }
    }
    case r: Remove => {
      val v = kv.get(r.key)
      if (v.isDefined) {
        kv = kv - (r.key)
        log.info("Replica - key " + r.key + " removed. Sending OperationAck to the client ")
        val p = Persist(r.key, None, r.id)
        savePersist(p, sender())
      } else {
        log.info("Replica - key " + r.key + " NOT removed. Sending OperationAck to the client ")
        sender() ! OperationAck(r.id)
      }
    }
    case m => {
      log.error("Replica - leader received unhandled message " + m + " from " + sender())
    }
  }

  private def savePersist(p: Persist, sender: ActorRef) = {
    kv = kv + ((p.key, StoreValue(p.valueOption, p.id)))
    childrenActors = childrenActors + context.actorOf(Props(classOf[ReplicaChild], sender, persistence, p))
    log.info("Replica - savePersist creating ReplicaChild " + p.id)
  }

  private def saveSnapshot(r: Snapshot, replicator: ActorRef): Unit = {
    log.info("Replica - saveSnapshot - start")

    kv = kv + ((r.key, StoreValue(r.valueOption, r.seq)))

    val p = Persist(r.key, r.valueOption, r.seq)

    log.info("Replica - saveSnapshot creating ReplicaChild " + r.seq)

    childrenActors = childrenActors + context.actorOf(Props(classOf[ReplicaChild], replicator, persistence, p))

  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case r: PoisonPill => {
      log.info("Replica - secondary received message: PoisonPill")
      childrenActors foreach (child => child ! PoisonPill)
      context.stop(self)
    }
    case r: ReplicaChildDead => {
      log.info("Replica - leader received message: ReplicaChildDead ")
      childrenActors = childrenActors - sender()
    }
    case r: PersistAck => {
      val snapshotAck = SnapshotAck(r.key, r.id)
      log.info("Replica - secondary received message: PersistAck")
      r.sender.tell(snapshotAck, this.self)
      log.info("Replica - secondary sent " + snapshotAck + " to " + r.sender)
    }
    case r: Snapshot => {
      log.info("Replica received message Snapshot " + r + " from " + sender())
      val v = kv.get(r.key)
      val kvId = v.getOrElse(StoreValue(null, minId))
      if (v.isDefined) {
        if (r.seq > kvId.id) {
          minId = r.seq
          log.info("Before saveSnapshot 2")
          saveSnapshot(r, sender())
        } else {
          val snapshotAck = SnapshotAck(r.key, r.seq)
          sender() ! SnapshotAck(r.key, r.seq)
          log.info("Replica sent message SnapshotAck " + snapshotAck + " to " + sender())
        }
      } else {
        if (r.seq == (kvId.id + 1)) {
          minId = r.seq
          log.info("Replica - Before saveSnapshot 3")
          saveSnapshot(r, sender())
        } else {
          log.info("Replica - should I discard saveSnapshot?")
        }
      }
    }
    case r: Get => {
      val res = kv.get(r.key).getOrElse(StoreValue(None, 0L))
      sender() ! GetResult(r.key, res.value, r.id)
    }
    case r: Insert => {
      sender() ! OperationFailed(r.id)
    }
    case r: Remove => {
      sender() ! OperationFailed(r.id)
    }
    case m => {
      log.error("Replica - secondary received unhandled message " + m + " from " + sender())
    }
  }

}

