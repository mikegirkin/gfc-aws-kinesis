package com.gilt.gfc.aws.kinesis.akka

import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Source, SourceQueue}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.gilt.gfc.aws.kinesis.client.KinesisRecordReader

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

object KinesisStreamSource {

  /**
    * Provides a way to pump Kinesis messages to materialized Akka stream
    * @param queue Materualized akka stream, which source is Source.queue[T]
    * @param timeOutDuration Maximum time for the akka stream to process messages
    * @return A message handler function to use in KinesisStreamHandler
    */
  def pumpKinesisStreamTo[T](queue: SourceQueue[T], timeOutDuration: Duration = Duration.Inf): (String, T, IRecordProcessorCheckpointer) => Unit = {
    (sharId: String, message: T, checkpointer: IRecordProcessorCheckpointer) => Await.result(queue.offer(message), timeOutDuration)
  }

  /**
    * Creates a non-materialized akka source connected to Kinesis stream
    * Upon materialization it will create a kinesis worker, and start consuming the Kinesis stream,
    * pumping messages to the underlying flow
    * @param streamConfig Configuration of the Kinesis stream to consume
    * @param pumpingTimeoutDuration Duration that source will wait for the akka stream to process message
    * @param bufferSize Size of the buffer to hold the incoming messages
    * @param overflowStrategy What to do with messages that would overflow the buffer e.g. backpressure or drop
    * @param evReader Deserialization typeclass
    * @param executionContext The execution context to be used for running the kinesis consumer
    * @return akka Source that on materialization gets messages from Kinesis stream and pump them through the flow
    */
  def apply[T](
    streamConfig: KinesisStreamConsumerConfig[T],
    pumpingTimeoutDuration: Duration = Duration.Inf,
    bufferSize : Int = 0,
    overflowStrategy: OverflowStrategy = OverflowStrategy.backpressure
  ) (
    implicit evReader: KinesisRecordReader[T],
    executionContext: ExecutionContext
  ) = {
    Source.queue[T](bufferSize, overflowStrategy)
      .mapMaterializedValue(queue => {
        val consumer = new KinesisStreamConsumer[T](streamConfig, KinesisStreamHandler(pumpKinesisStreamTo(queue, pumpingTimeoutDuration)))
        Future(consumer.run)
      })
  }
}
