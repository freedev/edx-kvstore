package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Props}
import akka.event.LoggingReceive
import kvstore.Persistence.{Persist, PersistAck, Persisted}
import kvstore.Replica.{OperationFailed, SendMessage}
import kvstore.Replicator.{Replicated, Snapshot, SnapshotAck}

import scala.concurrent.duration._

object ReplicatorChild {
  def props(parentActor:ActorRef, sender:ActorRef, replica: ActorRef, snapshot : Snapshot): Props = Props(classOf[ReplicaChild], parentActor, sender, replica, snapshot)
}

class ReplicatorChild(parentActor:ActorRef, sender:ActorRef, replica: ActorRef, snapshot : Snapshot) extends Actor with ActorLogging {

  log.info(s"ReplicatorChild - Started because of Replica")

  var cancellableSchedule:Option[Cancellable] = None
  var counter = 0

  override def preStart(): Unit = {
    log.info("ReplicatorChild - preStart")
    //    sendMessage()
  }

  override def postRestart(reason: Throwable): Unit = {
    super.postRestart(reason)
    log.info(s"ReplicatorChild - Restarted because of ${reason.getMessage}")
  }

  def receive = LoggingReceive {
    case r:SnapshotAck => {
      log.info("ReplicatorChild - Received SnapshotAck from " + sender())
      cancellableSchedule.foreach(c => c.cancel())
      sender ! Replicated(r.key, r.seq)
      context.stop(self)
    }
    case r:SendMessage => {
      log.info("ReplicatorChild - Received SendMessage from " + sender())
      //      sendMessage()
      sendScheduledMessage
    }
  }

  private def sendScheduledMessage(): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.language.postfixOps
    cancellableSchedule = Option(context.system.scheduler.schedule(0 milliseconds, 50 milliseconds){
      sendMessage()
    })
  }

  private def sendMessage(): Unit = {
    log.info("ReplicatorChild - sendMessage " + snapshot.getClass.getName + " to " + replica)
    counter = counter + 1
    if (counter < 20) {
      replica ! snapshot
    } else  {
      sender ! Replicated(snapshot.key, snapshot.seq)
      if (context != null)
        context.stop(self)
    }
  }

}
