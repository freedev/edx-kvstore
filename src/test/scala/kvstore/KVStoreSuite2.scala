package kvstore

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll

class KVStoreSuite2
  extends IntegrationSpec
    with Tools
    with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("KVStoreSuite")

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

}

