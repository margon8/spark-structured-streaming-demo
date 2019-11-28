/*
 * Copyright (c) 2018 Schibsted Media Group. All rights reserved
 */
package infrastructure.kafka

import java.time.Duration
import java.util.{Collections, Properties}

import infrastructure.kafka.KafkaDockerService._
import com.typesafe.scalalogging.LazyLogging
import com.whisk.docker._
import com.whisk.docker.config.DockerKitConfig
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}

import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.Random

object KafkaDockerService {
  val DefaultKafkaHost = "localhost"
  val DefaultKafkaPort = 9092
}
trait KafkaDockerService extends DockerKit with DockerKitConfig with LazyLogging {

  override val StartContainersTimeout: FiniteDuration = 1.minute

  lazy val adminClient: AdminClient = createAdminClient()

  val kafkaContainer: DockerContainer = configureDockerContainer("docker.kafka")

  abstract override def dockerContainers: List[DockerContainer] =
    kafkaContainer :: super.dockerContainers

  protected def givenExistingTopic(topic: String, numPartitions: Int = 1): Unit = {
    logger.info("Creating Kafka topic: {}", topic)
    adminClient
      .createTopics(Collections.singletonList(new NewTopic(topic, numPartitions, 1)))
      .all()
      .get()
    logger.info("Kafka topic: {} created", topic)

  }

  protected def publishToKafka(events: TraversableOnce[KafkaEvent]): Unit = {
    val producer: Producer[String, String] = createProducer()

    try events.foreach { event =>
      // scalastyle:off null
      println(s"Sending event $event to kafka")
      producer.send(new ProducerRecord(event.topic, null, event.timestamp, null, event.event)).get()
      // scalastyle:on null
    } finally producer.close()
  }

  protected def timedPublishToKafka(events: TraversableOnce[KafkaEvent]): Unit = {

    events.foldLeft(0L) { (lastEventTimestamp,event) =>
        Future {
          if (lastEventTimestamp != 0) Thread.sleep((event.timestamp - lastEventTimestamp)/15)
          println(s"Sending delayed event $event to kafka")
          // scalastyle:off null
          val producer: Producer[String, String] = createProducer()
          try producer.send(new ProducerRecord(event.topic, null, event.timestamp, null, event.event)).get()
          finally producer.close()
          // scalastyle:on null
        }
        event.timestamp
      }
  }

  protected def withTopicSubscription[A](topics: Seq[String])(
      block: Stream[ConsumerRecord[Array[Byte], String]] => A
  ): A = {
    val properties = getDefaultConsumerProperties

    val consumer = new KafkaConsumer[Array[Byte], String](properties)
    consumer.subscribe(topics.asJava)

    val stream = Stream
      .continually(consumer.poll(Duration.ofSeconds(1)))
      .flatMap(_.iterator().asScala)

    try block(stream)
    finally consumer.close()
  }

  private def getDefaultProducerProperties = {
    val properties = new Properties()
    // ProducerConfig.BOOTSTRAP_SERVERS_CONFIG
    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                   s"${DefaultKafkaHost}:${DefaultKafkaPort}")
    properties.put(ProducerConfig.ACKS_CONFIG, "all")
    properties.put(ProducerConfig.RETRIES_CONFIG, Integer.valueOf(0))
    properties.put(ProducerConfig.LINGER_MS_CONFIG, Integer.valueOf(1))
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                   "org.apache.kafka.common.serialization.StringSerializer")
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                   "org.apache.kafka.common.serialization.StringSerializer")

    properties
  }

  private def getDefaultConsumerProperties = {
    val properties = new Properties()
    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                   s"${DefaultKafkaHost}:${DefaultKafkaPort}")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, s"test_consumer_${Random.nextLong()}")
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                   "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                   "org.apache.kafka.common.serialization.StringDeserializer")

    properties
  }

  private def createAdminClient(): AdminClient = {
    val properties = new Properties()
    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                   s"${DefaultKafkaHost}:${DefaultKafkaPort}")
    properties.put("client.id", "test")
    AdminClient.create(properties)
  }

  private def createProducer(): Producer[String, String] = {
    val properties = getDefaultProducerProperties

    new KafkaProducer[String, String](properties)
  }

}
