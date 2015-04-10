name := "sqs-to-s3"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
  "com.typesafe.akka" % "akka-actor_2.11" % "2.3.9",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "org.slf4j" % "slf4j-simple" % "1.7.12",
  "com.amazonaws" % "aws-java-sdk" % "1.9.29",
  "com.typesafe" % "config" % "1.2.1"
)
