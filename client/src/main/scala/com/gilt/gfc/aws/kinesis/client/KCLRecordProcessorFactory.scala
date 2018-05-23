package com.gilt.gfc.aws.kinesis.client

import java.{util => jul}

import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorCheckpointer, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.model.Record
import com.gilt.gfc.logging.Loggable
import com.gilt.gfc.util.Retry

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.control.NonFatal

/**
 * Constructs IRecordProcessorFactory.
 *
 * See https://github.com/aws/aws-sdk-java/blob/master/src/samples/AmazonKinesisApplication/SampleKinesisApplication.java
 *     http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-record-processor-implementation-app-java.html
 *
 *  N.B. This only constructs IRecordProcessorFactory, if you want higher level entry point take a look at KCLWorkerRunner.
 */
object KCLRecordProcessorFactory {

  /** Quick scala translation of KCL example.
    *
    * Assumes we are mostly interested in specifying a record processing function, running with some number of
    * retries and sharing some required shutdown behavior.
    *
    * All parameters except for processRecords have defaults.
    *
    * @param checkpointInterval how often to save checkpoint to dynamodb
    * @param numRetries         how many times to retry operation on exception before giving up
    * @param initialize         (ShardId) => Unit : additional code to execute when handler is initialized
    * @param shutdown           (ShardId, Checkpointer, ShutdownReason) => Unit : additional code to execute on shutdown
    * @param processRecords     (ShardId, Records, Checkpointer) => Unit : Kinesis record handler
    * @param initialRetryDelay  the initial failed operation retry delay value, defaults to 10 seconds
    * @param maxRetryDelay      the maximum failed operation retry delay value, defaults to 3 minutes
    */
  def apply( checkpointInterval: Duration = 5 minutes
           , numRetries: Int = 3
           , initialize: (String) => Unit = (_) => ()
           , shutdown: (String, IRecordProcessorCheckpointer, ShutdownReason) => Unit = (_,_,_) => ()
           , initialRetryDelay: Duration = 10 seconds
           , maxRetryDelay: FiniteDuration = 3 minutes
          )( processRecords: (String, Seq[Record], IRecordProcessorCheckpointer) => Unit
           ): IRecordProcessorFactory = {

    new IRecordProcessorFactoryImpl(
      checkpointInterval
    , numRetries
    , initialize
    , shutdown
    , processRecords
    , initialRetryDelay
    , maxRetryDelay
    )
  }

  private
  class IRecordProcessorFactoryImpl(
    checkpointInterval: Duration
  , numRetries: Int
  , doInitialize: (String) => Unit
  , doShutdown: (String, IRecordProcessorCheckpointer, ShutdownReason) => Unit
  , doProcessRecords: (String, Seq[Record], IRecordProcessorCheckpointer) => Unit
  , initialRetryDelay: Duration
  , maxRetryDelay: FiniteDuration

  ) extends IRecordProcessorFactory
       with Loggable {

    override
    def createProcessor(): IRecordProcessor = new IRecordProcessor {

      var myShardId = "UNDEFINED"

      var lastCheckpointTimestamp = 0l


      override
      def initialize( shardId: String
                    ): Unit = {

        myShardId = shardId

        info(s"Initializing record handler for kinesis shard ${myShardId}")
        doRetry(doInitialize(myShardId))
      }


      override
      def shutdown( checkpointer: IRecordProcessorCheckpointer
                  , reason: ShutdownReason
                  ): Unit = {

        doRetry(doShutdown(myShardId, checkpointer, reason))

        info(s"Shutting down record processor for shard: ${myShardId}")

        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (reason == ShutdownReason.TERMINATE) {
          doCheckpoint(checkpointer)
        }
      }


      override
      def processRecords( recordsJList: jul.List[Record]
                        , checkpointer: IRecordProcessorCheckpointer
                        ): Unit = {

        val records: Seq[Record] = recordsJList.asScala
        debug(s"Processing ${records.size} records from shard ${myShardId}")

        try {
          doRetry {
            doProcessRecords(myShardId, records, checkpointer)
          }
        } catch {
          case NonFatal(e) =>
            // if it really got stuck badly - log error and move on, continue processing what we can
            error(s"SKIPPING ${records.size} records from shard ${myShardId} :: ${e.getMessage}", e)
        }

        // Checkpoint periodically
        if ( checkpointInterval.isFinite() &&  System.currentTimeMillis - lastCheckpointTimestamp > checkpointInterval.toMillis ) {
          doRetry{ doCheckpoint(checkpointer) }
        }
      }


      private[this]
      def doCheckpoint(checkpointer: IRecordProcessorCheckpointer
                      ): Unit = {

        lastCheckpointTimestamp = System.currentTimeMillis

        doRetry {
          checkpointer.checkpoint()
        }
      }


      private[this]
      def doRetry[R]( fun: => R
                    ): R = {
        implicit def log(t: Throwable): Unit = warn(t.getMessage)
        Retry.retryWithExponentialDelay(numRetries, initialDelay = initialRetryDelay, maxDelay = maxRetryDelay) {
          try { // Adds shard info to underlying exception, for debugging
            fun
          } catch {
            case NonFatal(e) =>
              throw new KCLProcessorException(s"Kinesis shard: ${myShardId} :: ${e.getMessage}", e)
          }
        }
      }

    }
  }


  private
  class KCLProcessorException(
    msg: String
  , t: Throwable
  ) extends RuntimeException(msg, t)

}
