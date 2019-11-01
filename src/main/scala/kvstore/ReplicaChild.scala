package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, PoisonPill, Props, Timers}
import akka.event.LoggingReceive
import kvstore.Persistence.{Persist, PersistAck, Persisted}
import kvstore.Replica.{OperationAck, OperationFailed, SendMessage}
import kvstore.ReplicaChild.ReplicaChildDead

import scala.concurrent.duration._

object ReplicaChild {

  case class ReplicaChildDead()

  def props(parentActor:ActorRef, sender:ActorRef, persistence: ActorRef, persist : Persist): Props = Props(classOf[ReplicaChild], parentActor, sender, persistence, persist)
}

class ReplicaChild(sender:ActorRef, persistence: ActorRef, persist : Persist)
  extends Actor
    with ActorLogging
    with Timers
{

  timers.startPeriodicTimer(persist.id, SendMessage(persist.key, persist.id), 100.millis)
  persistence ! persist

  var counter = 0

  override def preStart(): Unit = {
   // log.info("ReplicaChild - preStart " + self)
    //    sendMessage()
  }

  override def postRestart(reason: Throwable): Unit = {
    super.postRestart(reason)
   // log.info(s"ReplicaChild - Restarted because of ${reason.getMessage}")
  }

  def receive = LoggingReceive {
    case r: PoisonPill => {
      log.info("ReplicaChild - secondary received message: PoisonPill")
      timers.cancel(persist.id)
      context.stop(self)
    }
    case r:Persisted => {
     // log.info("ReplicaChild - Received Persisted from " + sender())
      context.parent ! PersistAck(sender, persist.key, persist.valueOption, persist.id)
      context.parent ! ReplicaChildDead()
      timers.cancel(persist.id)
      context.stop(self)
    }
    case r:SendMessage => {
     // log.info("ReplicaChild - Received SendMessage from " + sender())
      counter = counter + 1
      if (counter < 10) {
        persistence ! persist
      } else  {
        sender ! OperationFailed(persist.id)
        if (context != null) {
          context.parent ! ReplicaChildDead()
          context.stop(self)
        }
      }
    }
  }

}
