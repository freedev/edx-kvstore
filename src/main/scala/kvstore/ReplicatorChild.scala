package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, PoisonPill, Props, Timers}
import akka.event.LoggingReceive
import kvstore.Persistence.{Persist, PersistAck, Persisted}
import kvstore.Replica.{OperationFailed, SendMessage}
import kvstore.Replicator.{Replicated, Snapshot, SnapshotAck}
import kvstore.ReplicatorChild.ReplicatorChildDead

import scala.concurrent.duration._

object ReplicatorChild {

  case class ReplicatorChildDead()

  def props(sender:ActorRef, replica: ActorRef, snapshot : Snapshot): Props = Props(classOf[ReplicaChild], sender, replica, snapshot)
}

class ReplicatorChild(leader:ActorRef, replica: ActorRef, snapshot : Snapshot)
  extends Actor
  with ActorLogging
  with Timers
{

  timers.startPeriodicTimer(snapshot.seq, SendMessage(snapshot.key, snapshot.seq), 100.millis)
  replica ! snapshot

  var counter = 0

  override def preStart(): Unit = {
      log.info("ReplicatorChild - preStart " + self)
    //    sendMessage()
  }

  override def postRestart(reason: Throwable): Unit = {
    super.postRestart(reason)
      log.info(s"ReplicatorChild - Restarted because of ${reason.getMessage}")
  }

  def receive = LoggingReceive {
    case r: PoisonPill => {
   log.info("ReplicatorChild - secondary received message: PoisonPill")
      timers.cancel(snapshot.seq)
      context.parent ! ReplicatorChildDead()
      context.stop(self)
    }
    case r:SnapshotAck => {
   log.info("ReplicatorChild - Received SnapshotAck from " + sender())
      context.parent ! r // why?
      leader.tell(Replicated(r.key, r.seq), context.parent)
   log.info("ReplicatorChild - leader IS " + leader)
      timers.cancel(snapshot.seq)
      context.stop(self)
    }
    case r:SendMessage => {
        log.info("ReplicatorChild - Received SendMessage from " + sender())
      counter = counter + 1
      if (counter < 10) {
        replica ! snapshot
      } else  {
        //      sender ! Replicated(snapshot.key, snapshot.seq)
        if (context != null) {
          timers.cancel(snapshot.seq)
          context.stop(self)
        }
      }
    }
  }


}
