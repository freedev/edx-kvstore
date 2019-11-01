package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Timers}
import akka.event.LoggingReceive
import kvstore.MassiveReplicatorChild.{CheckReplicateStatus, MassiveReplicatorDead, RemovedReplica}
import kvstore.Replica.{Operation, OperationAck, OperationFailed, SendMessage}
import kvstore.Replicator.{Replicate, Replicated, Snapshot, SnapshotAck}

import scala.concurrent.duration._

object MassiveReplicatorChild {

  case class RemovedReplica(replica: ActorRef)
  case class CheckReplicateStatus(id: Long)
  case class MassiveReplicatorDead()

}

class MassiveReplicatorChild(var secondaries:Map[ActorRef, ActorRef], client:ActorRef, replicate : Replicate)
  extends Actor
  with ActorLogging
  with Timers
{

  timers.startSingleTimer(replicate.id, SendMessage(replicate.key, replicate.id), 0.millis)

  var ackSize = secondaries.size
  var counter = 0

  override def preStart(): Unit = {
   // log.info("ReplicatorChild - preStart " + self)
    //    sendMessage()
  }

  override def postRestart(reason: Throwable): Unit = {
    super.postRestart(reason)
   // log.info(s"ReplicatorChild - Restarted because of ${reason.getMessage}")
  }

  def checkAckCount():Unit = {
    ackSize = ackSize - 1
    if (ackSize == 0) {
      log.info("MassiveReplicatorChild - received message: Replicated")
      client ! OperationAck(replicate.id)
      context.parent ! MassiveReplicatorDead()
      timers.cancel(replicate.id)
      context.stop(self)
    }
  }

  def receive = LoggingReceive {
    case r: PoisonPill => {
      log.info("MassiveReplicatorChild - secondary received message: PoisonPill")
      timers.cancel(replicate.id)
      context.stop(self)
    }
    case r: RemovedReplica => {
      val replica = r.replica
      log.info("MassiveReplicatorChild - secondary received message: RemovedReplica " + replica )
      secondaries = secondaries - replica
      checkAckCount
    }
    case r: Replicated => {
      checkAckCount
    }
    case r: CheckReplicateStatus => {
      counter = counter + 1
      log.info("MassiveReplicatorChild - Received CheckReplicateStatus counter = " + counter)
      if (counter > 10) {
        client ! OperationFailed(r.id)
        timers.cancel(replicate.id)
        context.parent ! MassiveReplicatorDead()
        context.stop(self)
      }
    }
    case r:SendMessage => {
      log.info("MassiveReplicatorChild - Received SendMessage from " + sender())
      secondaries.foreach(element => {
        // log.info("Replica - sending Replicate to all replicators " + element._2)
        element._2.tell(replicate, self)
      })

      timers.startPeriodicTimer(replicate.id, CheckReplicateStatus(replicate.id), 100.millis)

    }
  }


}
