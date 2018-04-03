# gfc-aws-kinesis [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.gilt/gfc-aws-kinesis_2.12/badge.svg?style=plastic)](https://maven-badges.herokuapp.com/maven-central/com.gilt/gfc-aws-kinesis_2.12) [![Join the chat at https://gitter.im/gilt/gfc](https://badges.gitter.im/gilt/gfc.svg)](https://gitter.im/gilt/gfc?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Scala wrapper around AWS Kinesis Client Library. Part of the [Gilt Foundation Classes](https://github.com/gilt?q=gfc).

## Getting gfc-aws-kinesis

The latest version is 0.17.0, which is cross-built against Scala 2.11.x and 2.12.x.

SBT dependency:

```scala
libraryDependencies += "com.gilt" %% "gfc-aws-kinesis" % "0.17.0"
```

SBT Akka stream (2.5.x) dependency:

```scala
libraryDependencies += "com.gilt" %% "gfc-aws-kinesis-akka" % "0.17.0"
```

# Basic usage

Consume events:

```scala

  implicit object StringRecordReader extends KinesisRecordReader[String]{
    override def apply(r: Record) : String = new String(r.getData.array(), "UTF-8")
  }

  val config = KCLConfiguration("consumer-name", "kinesis-stream-name")

  KCLWorkerRunner(config).runAsyncSingleRecordProcessor[String](1 minute) { a: String =>
     // .. do something with A
     Future.successful(())
  }
```

Publish events:

```scala

  implicit object StringRecordWriter extends KinesisRecordWriter[String] {
    override def toKinesisRecord(a: String) : KinesisRecord = {
      KinesisRecord("partition-key", a.getBytes("UTF-8"))
    }
  }

  val publisher = KinesisPublisher()

  val messages = Seq("Hello World!", "foo bar", "baz bam")

  val result: Future[KinesisPublisherBatchResult] = publisher.publishBatch("kinesis-stream-name", messages)
```

# DynamoDB streaming

Create the adapter client
```scala
val streamAdapterClient: AmazonDynamoDBStreamsAdapterClient =
    new AmazonDynamoDBStreamsAdapterClient()
```

Pass the adapter client in the configuration
```scala
val streamSource = {
    val streamConfig = KinesisStreamConsumerConfig[Option[A]](
      applicationName,
      config.stream,
      regionName = Some(config.region),
      checkPointInterval = config.checkpointInterval,
      initialPositionInStream = config.streamPosition,
      dynamoDBKinesisAdapterClient = streamAdapterClient
    )
    KinesisStreamSource(streamConfig).mapMaterializedValue(_ => NotUsed)
  }
```

Pass an implicit kinesis record reader suitable for dynamodb events
```scala
implicit val kinesisRecordReader
      : KinesisRecordReader[Option[A]] =
      new KinesisRecordReader[Option[A]] {
        override def apply(record: Record): Option[A] = {
          record match {
            case recordAdapter: RecordAdapter =>
              val dynamoRecord: DynamoRecord =
                recordAdapter.getInternalObject
              dynamoRecord.getEventName match {
                case "INSERT" =>
                  ScanamoFree
                    .read[A](
                      dynamoRecord.getDynamodb.getNewImage)
                    .toOption
                case _ => None
              }
            case _ => None
          }
        }
      }
```

Consume e.g. using a sink

```scala
val targetSink = Sink.actorRefWithAck(target, startMsg, ackMsg, Done)

streamSource
  .filter(!_.isEmpty)
  .map(_.get)
  .log(applicationName)(log)
  .runWith(targetSink)
```


