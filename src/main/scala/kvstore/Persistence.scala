package kvstore

import akka.actor.{Actor, ActorRef, Props}

import scala.util.Random
import java.util.concurrent.atomic.AtomicInteger

import akka.event.Logging

object Persistence {
  case class Persist(key: String, valueOption: Option[String], id: Long)
  case class Persisted(key: String, id: Long)
  case class PersistAck(sender: ActorRef, key: String, valueOption: Option[String], id: Long)

  class PersistenceException extends Exception("Persistence failure")

  def props(flaky: Boolean): Props = Props(classOf[Persistence], flaky)
}

class Persistence(flaky: Boolean) extends Actor {
  import Persistence._

  val log = Logging(context.system, this)

  def receive = {
    case Persist(key, _, id) => {
     // log.info("Persistence received message Persist")
      if (!flaky || Random.nextBoolean()) sender ! Persisted(key, id)
      else throw new PersistenceException
    }
  }

}
