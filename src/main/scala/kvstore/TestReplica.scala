package kvstore

import akka.actor.{Actor, ActorRef, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated}
import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import akka.pattern.{Patterns, RetrySupport, ask}
import akka.util.Timeout
import kvstore.Replica.Operation

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object TestReplica {
  sealed trait Operation {
    def key: String
    def id: Long
  }

  case class StoreValue(value: Option[String], id: Long)
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(persistenceProps: Props): Props = Props(new TestReplica(persistenceProps))
}

class TestReplica(persistenceProps: Props) extends Actor {
  import Persistence._
  import TestReplica._
  import context.dispatcher

  val log = Logging(context.system, this)

  val persistence = context.actorOf(persistenceProps)

  var kv = Map.empty[String, StoreValue]

  def receive = {
    case r:Persisted => {
      log.info("TestReplica - secondary received UNHANDLED message: Persisted")
    }
    case r:Get => {
      val res = kv.get(r.key).getOrElse(StoreValue(None, 0L))
      sender() ! GetResult(r.key, res.value, r.id)
    }
    case r:Insert => {
      val client = context.sender()
      log.info("TestReplica received message Insert from client " + client)
      implicit val scheduler=context.system.scheduler
      val p = Persist(r.key, Some(r.value), r.id)
      val storeValue = StoreValue(Some(r.value), r.id)

      RetrySupport.retry(() => {
        log.info("TestReplica - sent message Persist to Persistence " + client)
        Patterns.ask(persistence, p, 50 millisecond)
      }, 16, 50 millisecond)
      .onSuccess({
        case p: Persisted => {
          log.info("TestReplica - Received Persisted so now sending an OperationAck to client " + client)
          kv = kv + ((p.key, storeValue))
          client ! OperationAck(p.id)
        }
      })

    }
    case _ =>
  }

}

