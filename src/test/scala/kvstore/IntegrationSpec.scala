/**
 * Copyright (C) 2013-2015 Typesafe Inc. <http://www.typesafe.com>
 */
package kvstore

import akka.testkit.TestProbe
import kvstore.Persistence.{Persist, Persisted}
import kvstore.TestReplica.{Insert, OperationAck}
import org.scalatest.{FunSuiteLike, Matchers}

import scala.concurrent.duration._

trait IntegrationSpec
  extends FunSuiteLike
        with Matchers
{ this: KVStoreSuite2 =>

  test("IntegrationSpec-case2: Primary retries persistence every 100 milliseconds") {
    val client = TestProbe()
    val persistence = TestProbe()

    val primary = system.actorOf(TestReplica.props(probeProps(persistence)), "step1-case2-primary")

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

    // client.expectNoMessage(100.milliseconds)

    persistence.reply(Persisted("foo", persistId))

    client.expectMsg(OperationAck(id))

  }

}
