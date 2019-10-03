package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Props}
import akka.event.LoggingReceive
import kvstore.Persistence.{Persist, PersistAck, Persisted}
import kvstore.Replica.{OperationAck, OperationFailed, SendMessage}

import scala.concurrent.duration._

object ReplicaChild {
  def props(parentActor:ActorRef, sender:ActorRef, persistence: ActorRef, persist : Persist): Props = Props(classOf[ReplicaChild], parentActor, sender, persistence, persist)
}

class ReplicaChild(parentActor:ActorRef, sender:ActorRef, persistence: ActorRef, persist : Persist) extends Actor with ActorLogging {

  log.info(s"ReplicaChild - Started because of Replica")

  var cancellableSchedule:Option[Cancellable] = None
  var counter = 0

  override def preStart(): Unit = {
    log.info("ReplicaChild - preStart " + self)
    //    sendMessage()
  }

  override def postRestart(reason: Throwable): Unit = {
    super.postRestart(reason)
    log.info(s"ReplicaChild - Restarted because of ${reason.getMessage}")
  }

  def receive = LoggingReceive {
    case r:Persisted => {
      log.info("ReplicaChild - Received Persisted from " + sender())
      cancellableSchedule.foreach(c => c.cancel())
      parentActor ! PersistAck(sender, persist.key, persist.valueOption, persist.id)
      context.stop(self)
    }
    case r:SendMessage => {
      log.info("ReplicaChild - Received SendMessage from " + sender())
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
    log.info("ReplicaChild - sendMessage " + persist.getClass.getName + " to " + persistence)
    counter = counter + 1
    if (counter < 20) {
      persistence ! persist
    } else  {
      sender ! OperationFailed(persist.id)
      if (context != null)
        context.stop(self)
    }
  }

}
