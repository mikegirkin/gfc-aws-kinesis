package com.gilt.gfc.aws.kinesis.client

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration, ShutdownReason, Worker}
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory
import com.amazonaws.services.kinesis.model.Record
import com.gilt.gfc.logging.Loggable
import java.util.concurrent.TimeUnit

import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
 * A helper class to merge required/optional/default parameters and run a KCL Worker.
 *
 * @param config             KCL config, @see KCLConfiguration for some useful defaults.
 * @param checkpointInterval how often to save checkpoint to dynamodb
 * @param numRetries         how many times to retry operation on exception before giving up
 * @param initialize         (ShardId) => Unit : additional code to execute when handler is initialized
 * @param shutdown           (ShardId, Checkpointer, ShutdownReason) => Unit : additional code to execute on shutdown
 * @param metricsFactory     CloudWatch metrics factory
 * @param initialRetryDelay  the initial failed operation retry delay value, defaults to 10 seconds
 * @param maxRetryDelay      the maximum failed operation retry delay value, defaults to 3 minutes
 */
case class KCLWorkerRunner (
  config: KinesisClientLibConfiguration
, dynamoDBKinesisAdapter: Option[AmazonDynamoDBStreamsAdapterClient] = None
, checkpointInterval: Duration = 5 minutes
, numRetries: Int = 3
, initialize: (String) => Unit = (_) => ()
, shutdown: (String, IRecordProcessorCheckpointer, ShutdownReason) => Unit = (_,_,_) => ()
, metricsFactory: Option[IMetricsFactory] = None
, initialRetryDelay: Duration = 10 seconds
, maxRetryDelay: FiniteDuration = 3 minutes
) extends Loggable {

  private[this] var workers = List[Worker]()

  /** Override default checkpointInterval. */
  def withCheckpointInterval( cpi: Duration
                            ): KCLWorkerRunner = {

    this.copy(checkpointInterval = cpi)
  }


  /** Override default num retries. */
  def withNumRetries( n: Int
                    ): KCLWorkerRunner = {

    this.copy(numRetries = n)
  }


  /** Override default (NOOP) init function. */
  def withInitialize( init: (String) => Unit
                    ): KCLWorkerRunner = {

    this.copy(initialize = init)
  }


  /** Override default (NOOP) shutdown function. */
  def withShutdown( sd: (String, IRecordProcessorCheckpointer, ShutdownReason) => Unit
                  ): KCLWorkerRunner = {

    this.copy(shutdown = sd)
  }

  def withMetricsFactory(factory: IMetricsFactory): KCLWorkerRunner = {
    this.copy(metricsFactory = Some(factory))
  }

  /**
   * Request graceful shutdown of all Kinesis workers.
   *
   * @param timeout   how long to wait for shutdown to complete
   */
  def shutdown(timeout: Duration = 1.minute): Unit = {
    val javaFutures = synchronized {
      val r = workers.map(_.requestShutdown)
      workers = Nil
      r
    }
    if (timeout.isFinite) {
      val endTime = System.nanoTime + timeout.toNanos
      javaFutures.foreach(_.get(endTime - System.nanoTime, TimeUnit.NANOSECONDS))
    } else {
      javaFutures.foreach(_.get)
    }
  }

  /**
   * Run KCL worker with the given callback.
   *
   * @param processRecords     (ShardId, Records, Checkpointer) => Unit : Kinesis record handler
   * @param evReader           evidence that A has implementation of KinesisRecordReader implicitly available in scope
   */
  def runBatchProcessor[A]( processRecords: (String, Seq[A], IRecordProcessorCheckpointer) => Unit
                         )( implicit evReader: KinesisRecordReader[A]
                          ): Unit = {
    try {

      val recordProcessorFactory = KCLRecordProcessorFactory(
        checkpointInterval = checkpointInterval
      , numRetries = numRetries
      , initialize = initialize
      , shutdown = shutdown
      , initialRetryDelay = initialRetryDelay
      , maxRetryDelay = maxRetryDelay
      ) { (shardId, records, checkpointer) =>

        val (as,errs) = records.map(tryToConvertRecord[A] _).partition(_.isSuccess)

        // process what we could parse, if this call throws an exception - the whole batch will be retried
        processRecords(shardId, as.map(_.get), checkpointer)

        // log records we could not parse, pointless to retry them
        errs.map(_.failed.get).foreach { e => error("Error processing a single batched record", e) }
      }

      val workerBuilder: Worker.Builder = new Worker.Builder()
            .recordProcessorFactory(recordProcessorFactory)
              .config(config)
      dynamoDBKinesisAdapter.foreach(
        adapter => workerBuilder.kinesisClient(adapter)
      )
      metricsFactory.foreach(mf => workerBuilder.metricsFactory(mf))

      val worker = workerBuilder.build()

      synchronized {
        workers ::= worker
      }
      worker.run()

    } catch {
      case NonFatal(e) =>
        error("Error processing an entire record batch", e)
    }
  }

  /**
   * Run KCL worker with the given callback.
   * Simple single-threaded execution, access to shard ID and checkpointer.
   *
   * @param processRecord     (ShardId, Record, Checkpointer) => Unit : Kinesis record handler
   * @param evReader          evidence that A has implementation of KinesisRecordReader implicitly available in scope
   */
  def runSingleRecordProcessor[A]( processRecord: (String, A, IRecordProcessorCheckpointer) => Unit
                                )( implicit evReader: KinesisRecordReader[A]
                                 ): Unit = {

    runBatchProcessor[A] { (shardId, as, checkpointer) => as.foreach(a => processRecord(shardId, a, checkpointer)) }
  }


  /**
   * Run KCL worker with the given asynchronous callback.
   * Batch will be processed in parallel.
   * You can control the level of parallelism by configuring provided execution context parameter.
   *
   * @param callbackTimeout   how long to wait for async call results
   * @param processRecord     (Record) => Future[Unit] : async record handler
   * @param executor          where to execute record processing functions
   * @param evReader          evidence that A has implementation of KinesisRecordReader implicitly available in scope
   */
  def runAsyncSingleRecordProcessor[A]( callbackTimeout: FiniteDuration
                                     )( processRecord: (A) => Future[Unit]
                                     )( implicit executor: ExecutionContext
                                      ,          evReader: KinesisRecordReader[A]
                                      ): Unit = {

    runBatchProcessor[Record]({ (shardId, records, checkpointer) =>

      val resFutures: Future[Seq[(Record, Try[Unit])]] = Future.traverse(records) { r =>

        debug(s"Got ${r.toString} from kinesis shard ${shardId}")

        tryToConvertRecord[A](r) match {
          case Failure(e) =>
            Future.successful(r -> Failure(e))

          case Success(a) =>
            val res : Future[(Record, Try[Unit])] = processRecord(a) map (_ => r -> Success(Unit))

            res recover {
              case NonFatal(e) => r -> Failure(e)
            }
        }
      }

      val results = Await.result(resFutures, callbackTimeout)

      // Log each result individually
      results.foreach { case (r, res) =>
        res match {
          case Success(_) =>
            debug(s"Successfully processed ${r.toString} from kinesis shard ${shardId}.")

          case Failure(e) =>
            e match {
              case ce: KCLWorkerRunnerRecordConversionException =>
                error(s"Skipped a record from kinesis shard ${shardId}", ce)

              // this runs in a context where same record batch will be retried a few times
              case NonFatal(e) =>
                throw KCLWorkerRunnerRecordProcessingException(r, shardId, e)
            }
        }
      }
    })(Implicits.IdentityKinesisRecordReader) // we'll convert on the worker thread
  }



  /** Adds a bit more context to failed attempts to convert kinesis records. */
  private[this]
  def tryToConvertRecord[A](r: Record
                          )( implicit evReader: KinesisRecordReader[A]
                           ): Try[A] = {

    val copyOfTheData = r.getData.duplicate // they are mutable, can only read once, see hexData

    try {
      Success(evReader(r))
    } catch {
      case NonFatal(e) =>
        debug(s"Record conversion failure - record [$r], raw data [${ByteBufferUtil.toHexString(copyOfTheData)}]", e)
        Failure(KCLWorkerRunnerRecordConversionException(r, e))
    }
  }
}


case class KCLWorkerRunnerRecordConversionException(
  record: Record
, cause: Throwable
) extends RuntimeException(s"Failed to convert ${record} to required type", cause)


case class KCLWorkerRunnerRecordProcessingException(
  record: Record
, shardId: String
, cause: Throwable
) extends RuntimeException(s"Failed to process ${record} from shard ${shardId}", cause)
