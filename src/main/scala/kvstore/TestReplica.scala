package kvstore

import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import akka.pattern.{CircuitBreaker, RetrySupport, ask}
import akka.util.Timeout
import kvstore.Arbiter.{JoinedPrimary, _}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}


class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Persistence._
  import Replica._
  import Replicator._
  import context.dispatcher

  val log = Logging(context.system, this)

  val persistence = context.actorOf(persistenceProps)

  arbiter.tell(Join, this.self)

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
    case r:Persisted => {
      log.info("Replica - leader received UNHANDLED message: Persisted")
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
          saveInsert(r, sender())
        } else {
          log.info("Replica - key " + r.key + " request id is lower than saved. NOT Saving. Sending back OperationFailed to the client.")
          sender() ! OperationFailed(r.id)
        }
      } else {
        log.info("Replica - key " + r.key + " is not Defined. Saving...")
        saveInsert(r, sender())
      }
    }
    case r:Remove => {
      val v = kv.get(r.key)
      if (v.isDefined) {
        kv  = kv - (r.key)
        log.info("Replica - key " + r.key + " removed. Sending OperationAck to the client ")
        sender() ! OperationAck(r.id)
      } else {
        log.info("Replica - key " + r.key + " NOT removed. Sending OperationFailed to the client ")
        sender() ! OperationFailed(r.id)
      }
    }
    case _ =>
  }

  def notifyMeOnOpen(): Unit =
    log.warning("My CircuitBreaker is now open")
  def notifyMeOnClose(): Unit =
    log.warning("My CircuitBreaker is now Closed")
  def notifyMeOnHalfOpen(): Unit =
    log.warning("My CircuitBreaker is now HalfOpen")

  private def saveInsert(r: Insert, sender: ActorRef) = {
    val storeValue = StoreValue(Some(r.value), r.id)
    val p = Persist(r.key, Some(r.value), r.id)
    implicit val timeout = Timeout(800.milliseconds)
    implicit val scheduler=context.system.scheduler

    log.info("Replica - sent message Persist to Persistence " + r.id)

    val cb = CircuitBreaker(context.system.scheduler, maxFailures = 16, callTimeout = 50.millisecond, resetTimeout = 800.milliseconds)
      .onOpen(notifyMeOnOpen())
      .onHalfOpen(notifyMeOnHalfOpen())
      .onClose(notifyMeOnClose())
      .onCallTimeout(l => {
        log.info("Replica - CircuitBreaker sent message CallTimeout " + l)
      })

    val future = cb.withCircuitBreaker( {
      log.info("Replica - CircuitBreaker trying to message send Persist to Persistence (for Insert)")
      persistence ask p
    } )

//    val future = RetrySupport.retry(() => {
//      log.info("sent message Persist to Persistence " + p)
//      Patterns.ask(persistence, p, 50 millisecond)
//    }, 16, 50 millisecond)

    future onComplete (t => {
      t match {
        case Failure(e) => {
          log.info("Replica - onComplete future returned Failure " + e)
        }
        case Success(v) => {
          log.info("Replica - onComplete future returned Success " + v)
        }
      }
    })
    future onFailure {
      case _ => {
        log.info("Replica - saveInsert future returned OperationFailed")
        sender ! OperationFailed(p.id)
      }
    }
    future onSuccess {
      case p: Persisted => {
        log.info("Replica - Received Persisted so now sending an OperationAck to Replicator")
        kv = kv + ((p.key, storeValue))
        sender ! OperationAck(p.id)
        secondaries.values.foreach(secondaryReplicator => {
          log.info("Replica - sending snapshot to all replicators " + secondaryReplicator)
          secondaryReplicator.tell(Replicate(p.key, storeValue.value, storeValue.id), secondaryReplicator)
        })
      }
      case _ => {
        log.info("Replica - saveInsert - Replica received something from persistence")
      }
    }
  }

//  private def myAsk(p : Persist) : Future[Any] = {
//    log.info("Replica - Retry to Persist")
//    val persistence = context.actorOf(persistenceProps)
//    implicit val timeout = Timeout(800.milliseconds)
////    Patterns.ask(persistence, p, 50 millisecond)
//    persistence ? p
//  }

//  override val supervisorStrategy =
//    OneForOneStrategy(maxNrOfRetries = -1, withinTimeRange = Duration.Inf) {
//      case _: Exception => Restart
//    }
//
//  var numFetchRetries = 0

  private def saveSnapshot(r: Snapshot, replicator: ActorRef):Future[Any] = {
    log.info("Replica - saveSnapshot - start")

    kv = kv + ((r.key, StoreValue(r.valueOption, r.seq)))

    val p = Persist(r.key, r.valueOption, r.seq)

    implicit val scheduler=context.system.scheduler
    implicit val timeout = Timeout(800.milliseconds)

    //Given some future that will succeed eventually
      val future = RetrySupport.retry(() => {
        log.info("Retry to Persist")
        persistence ? p
      }, 6, 50.millisecond)

//      val cb = CircuitBreaker(context.system.scheduler, maxFailures = 16, callTimeout = 50.millisecond, resetTimeout = 50.milliseconds)
//        .onOpen(notifyMeOnOpen())
//        .onHalfOpen(notifyMeOnHalfOpen())
//        .onClose(notifyMeOnClose())
//        .onCallTimeout(l => {
//          log.info("Replica - CircuitBreaker sent message CallTimeout " + l)
//        })
//
//      val defineFailureFn: Try[Any] ⇒ Boolean = {
//        case Success(v) => {
//          v match {
//            case e => {
//              log.info("Replica - saveSnapshot - Success 1 " + e)
//              true
//            }
//          }
//        }
//        case Failure(f) => {
//          f match {
//            case e: AskTimeoutException => {
//              log.info("Replica - saveSnapshot - Failure 1 " + e)
//              false
//            }
//            case e => {
//              log.info("Replica - saveSnapshot - Failure 2 " + e)
//              true
//            }
//          }
//        }
//          false
//      }
//
//      val future = cb.withCircuitBreaker( {
//        log.info("Replica - CircuitBreaker trying to message send Persist to Persistence (for Snapshot)")
////        persistence ? p
//        ask(persistence, p)
//      }
//        , defineFailureFn
//      )

//      future.pipeTo(self).to(self)

      //    log.info("withCircuitBreaker... end")
//    future onFailure  {
//      case e: AskTimeoutException => {
//        log.info("Replica - saveSnapshot - onFailure 1 ")
//      }
//      case _ => {
//        log.info("Replica - saveSnapshot - onFailure 2 ")
//      }
//    }
    future onSuccess {
      case p1: Persisted => {
        log.info("Replica received Persisted - send back to sender SnapshotAck(" + p1.key + "," + p1.id + ")")
        replicator.tell(SnapshotAck(p1.key, p1.id), this.self)
      }
      case p2 => {
        log.info("saveSnapshot - Replica received something from persistence " + p2 )
      }
    }
//    future.onComplete(t => {
//      log.info("Replica onComplete - (" + t + ")")
//    })
    log.info("Replica - saveSnapshot - end")
    future
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case r:Persisted => {
      log.info("Replica - secondary received UNHANDLED message: Persisted")
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
    case _ =>
  }

}

