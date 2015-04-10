package com.krumptium.sqs2s3

import akka.actor._
import com.krumptium.sqs2s3.actors.WorkerActor
import com.krumptium.sqs2s3.actors.WorkerActor._

object Sqs2S3 extends App {
  val system = ActorSystem("my-system")

  val worker = system.actorOf(WorkerActor.props)

  worker ! Initialize

  system.awaitTermination()
}
