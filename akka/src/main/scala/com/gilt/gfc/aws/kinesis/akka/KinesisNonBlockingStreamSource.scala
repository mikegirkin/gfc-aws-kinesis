package com.gilt.gfc.aws.kinesis.akka

import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Source, SourceQueue}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.gilt.gfc.aws.kinesis.client.KinesisRecordReader

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

@deprecated("This API is deprecated, use KinesisStreamSource instead", "0.17.0")
object KinesisNonBlockingStreamSource {

  /**
    * Provides a way to pump Kinesis messages to materialized Akka stream
    *
    * @deprecated Use [[com.gilt.gfc.aws.kinesis.akka.KinesisStreamSource]] instead
    *
    * @param queue Materualized akka stream, which source is Source.queue[T]
    * @param timeOutDuration Maximum time for the akka stream to process messages
    * @return A message handler function to use in KinesisStreamHandler
    */
  @deprecated("This API is deprecated, use KinesisStreamSource instead", "0.17.0")
  def pumpKinesisStreamTo[T](queue: SourceQueue[T], timeOutDuration: Duration = Duration.Inf): (String, T, IRecordProcessorCheckpointer) => Unit = {
    (sharId: String, message: T, checkpointer: IRecordProcessorCheckpointer) => Await.result(queue.offer(message), timeOutDuration)
  }

  /**
    * Creates a non-materialized akka source connected to Kinesis stream
    * Upon materialization it will create a kinesis worker, and start consuming the Kinesis stream,
    * pumping messages to the underlying flow
    *
    * @deprecated Use [[com.gilt.gfc.aws.kinesis.akka.KinesisStreamSource]] instead
    *
    * @param streamConfig Configuration of the Kinesis stream to consume
    * @param pumpingTimeoutDuration Duration that source will wait for the akka stream to process message
    * @param bufferSize Size of the buffer to hold the incoming messages
    * @param overflowStrategy what to do with messages that would overflow the buffer e.g. backpressure or drop
    * @param evReader Deserialization typeclass
    * @param ec the execution context to be used for running the kinesis consumer
    * @return akka Source that on materialization gets messages from Kinesis stream and pump them through the flow
    */
  @deprecated("This API is deprecated, use KinesisStreamSource instead", "0.17.0")
  def apply[T](
                streamConfig: KinesisStreamConsumerConfig[T],
                pumpingTimeoutDuration: Duration = Duration.Inf,
                bufferSize : Int = 0,
                overflowStrategy: OverflowStrategy = OverflowStrategy.backpressure
              ) (
                implicit evReader: KinesisRecordReader[T],
                ec : ExecutionContext) = {
    KinesisStreamSource(streamConfig, pumpingTimeoutDuration, bufferSize, overflowStrategy)
  }

}
