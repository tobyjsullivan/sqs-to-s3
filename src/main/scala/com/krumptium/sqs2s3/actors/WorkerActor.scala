package com.krumptium.sqs2s3.actors

import java.io.{FileWriter, BufferedWriter, Writer, File}

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.sqs.AmazonSQSClient
import com.amazonaws.services.sqs.model.{DeleteMessageBatchRequestEntry, DeleteMessageBatchRequest, Message, ReceiveMessageRequest}
import com.typesafe.scalalogging.Logger
import akka.actor._
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import scala.concurrent.duration._
import com.typesafe.config.{Config, ConfigFactory}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConversions._

object WorkerActor {
  val logger = Logger(LoggerFactory.getLogger(""))

  val conf: Config = ConfigFactory.load()
  val awsAccessKey = conf.getString("aws.iam.accessKeyId")
  val awsSecretKey = conf.getString("aws.iam.secretAccessKey")
  val awsCredentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey)

  val sqsQueueUrl = conf.getString("aws.sqs.queueUrl")
  lazy val sqsClient = new AmazonSQSClient(awsCredentials)

  val s3BucketName = conf.getString("aws.s3.bucketName")
  val s3KeyPrefix = conf.getString("aws.s3.keyPrefix")
  lazy val s3Client = new AmazonS3Client(awsCredentials)

  def props: Props = Props(classOf[WorkerActor])

  case object Initialize
  case object ProcessNextBatch
  case object UploadFile
  case object Shutdown
}

class WorkerActor extends Actor {
  import WorkerActor._


  var currentFile: File = null
  var currentWriter: Writer = null

  def receive = {
    case Initialize =>
      logger.info("Initializing")
      openNewTempFile()
      context.system.scheduler.scheduleOnce(60 seconds, self, UploadFile)
      self ! ProcessNextBatch
    case ProcessNextBatch =>
      val lines = readNextBatch()
      if (currentWriter == null)
        openNewTempFile()
      writeLines(currentWriter, lines)
      self ! ProcessNextBatch
    case UploadFile =>
      if (currentWriter != null && currentFile != null)
        closeAndUploadFile()
      context.system.scheduler.scheduleOnce(60 seconds, self, UploadFile)
    case Shutdown =>
      self ! UploadFile
      self ! PoisonPill
  }

  private def newTempFile(): File = File.createTempFile("sqs-data", ".json")
  private def openFile(file: File): Writer = new BufferedWriter(new FileWriter(file))

  private def openNewTempFile(): Unit = {
    currentFile = newTempFile()
    currentWriter = openFile(currentFile)
  }

  private def closeAndUploadFile() = {
    currentWriter.close()
    currentWriter = null
    if (currentFile.length() > 0)
      uploadFileToS3(currentFile)
    currentFile.delete()
    currentFile = null
  }

  private def uploadFileToS3(file: File): Unit = {
    logger.info("Processing file: "+file.getName)
    s3Client.putObject(s3BucketName, buildS3KeyPrefix()+file.getName, file)
  }

  private def buildS3KeyPrefix(): String = {
    val dateString = DateTime.now().toString("yyyy-MM-dd")

    s3KeyPrefix + dateString + "/"
  }

  private def readNextBatch(): Seq[String] = {
    val receiveMessageRequest = new ReceiveMessageRequest(sqsQueueUrl)
    receiveMessageRequest.setMaxNumberOfMessages(10)
    val messages: List[Message] = sqsClient.receiveMessage(receiveMessageRequest).getMessages.toList

    // Delete any received messages
    if (messages.nonEmpty) {
      val deleteRequest = new DeleteMessageBatchRequest(sqsQueueUrl, messages.map { message =>
        new DeleteMessageBatchRequestEntry(message.getMessageId, message.getReceiptHandle)
      })
      sqsClient.deleteMessageBatch(deleteRequest)
    }

    messages.map(_.getBody)
  }

  private def writeLines(writer: Writer, lines: Seq[String]): Unit =
    lines.foreach { line =>
      logger.info("Writing line: "+line)
      writer.write(line)
    }
}
