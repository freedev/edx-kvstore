/**
 * Copyright (C) 2013-2015 Typesafe Inc. <http://www.typesafe.com>
 */
package kvstore

import akka.actor.Props
import akka.testkit.TestProbe
import kvstore.Persistence.{Persist, Persisted}
import kvstore.Replica.{Insert, OperationAck}
import org.scalatest.{FunSuiteLike, Matchers}

import scala.concurrent.duration._

trait IntegrationSpec
  extends FunSuiteLike
        with Matchers
{ this: KVStoreSuite2 =>

  test("Integration-case1") {
    val arbiter = system.actorOf(Props(classOf[Arbiter]), "integration-case1-arbiter")
    val primary = system.actorOf(Replica.props(arbiter, Persistence.props(flaky = true)), "integration-case1-primary")
    val client = session(primary)
    client.setAcked("k1", "v1")
  }
//
//  test("Integration-case2") {
//    val arbiter = system.actorOf(Props(classOf[Arbiter]), "integration-case2-arbiter")
//    val primary = system.actorOf(Replica.props(arbiter, Persistence.props(flaky = true)), "integration-case2-primary")
//    val client = session(primary)
//    client.get("k1") === None
//
//    //TODO use kvstore.given.Arbiter instead of Unreliable
//    val unreliableReplica = system.actorOf(Unreliable.props(Replica.props(arbiter, Persistence.props(flaky = true))), "integration-case2-unreliable-replica")
//    client.setAcked("k1", "v1")
//  }

  test("IntegrationSpec-case2: Primary retries persistence every 100 milliseconds") {
    val client = TestProbe()
    val arbiter = system.actorOf(Props(classOf[Arbiter]), "integration-case1-arbiter")
    val persistence = TestProbe()

    val primary = system.actorOf(Replica.props(arbiter, probeProps(persistence)), "step1-case2-primary")

    val id = 0

    client.send(primary, Insert("foo", "bar", id))

    client.expectNoMessage(100.milliseconds)

    val persistId = persistence.expectMsgPF() {
      case Persist("foo", Some("bar"), id) => id
    }

    assert(persistId == id)
    // Retries form above
    persistence.expectMsg(200.milliseconds, Persist("foo", Some("bar"), persistId))
    persistence.expectMsg(200.milliseconds, Persist("foo", Some("bar"), persistId))

    persistence.reply(Persisted("foo", persistId))

    client.expectMsg(OperationAck(id))

  }

}
