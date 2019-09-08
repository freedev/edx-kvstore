/**
 * Copyright (C) 2013-2015 Typesafe Inc. <http://www.typesafe.com>
 */
package kvstore

import akka.actor.Props
import akka.testkit.TestProbe
import org.scalatest.{FunSuiteLike, Matchers}

trait IntegrationSpec
  extends FunSuiteLike
        with Matchers { this: KVStoreSuite =>

  import Arbiter._

  /*
   * Recommendation: write a test case that verifies proper function of the whole system,
   * then run that with flaky Persistence and/or unreliable communication (injected by
   * using an Arbiter variant that introduces randomly message-dropping forwarder Actors).
   */

  test("IntegrationSpec-case1: verifies proper function of the whole system") {
    val arbiter = TestProbe()
    val primary = system.actorOf(Replica.props(arbiter.ref, Persistence.props(flaky = false)), "integration-spec-case1")
    val client = session(primary)

    arbiter.expectMsg(Join)
    ()
  }

  }
