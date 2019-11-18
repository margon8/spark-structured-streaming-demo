package infrastructure.kafka

/*
 * Copyright (c) 2018 Schibsted Media Group. All rights reserved
 */
import java.util
import java.util.Properties

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}

import scala.collection.JavaConverters._
import scala.collection.mutable

class KafkaService(consumer: KafkaConsumer[String, String], kafkaConfiguration: Config)
    extends LazyLogging {
  private implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

  val subscribeTopics = "subscribeTopics"

  def getTopics(topicRegex: String = kafkaConfiguration.getString(subscribeTopics)): Seq[String] = {

    val publicTopicList = consumer
      .listTopics()
      .keySet()
      .asScala
      .toSeq
      .sorted

    publicTopicList.filter(t => t.matches(topicRegex))
  }

  def offsetRangesByDatetime(topic: String,
                             startTime: Long,
                             endTime: Long): TopicAndOffsets = {

    val offsetsEndMap: Map[TopicPartition, Long] = endOffsets(topic)

    val lowLimitOffsets: Map[TopicPartition, Long] =
      topicPartitionsWithTimestamps(topic, startTime)
    val highLimitOffsets: Map[TopicPartition, Long] =
      topicPartitionsWithTimestamps(topic, endTime)

    logger.info("Topic: {} end offsets: {}", topic, offsetsEndMap)
    logger.info("Topic: {}, startTime: {}, lowLimitOffsets: {}", topic, startTime, lowLimitOffsets)
    logger.info("Topic: {}, endTime: {},  highLimitOffsets: {}", topic, endTime, highLimitOffsets)

    val startOffsets: Map[TopicPartition, Long] = if (lowLimitOffsets.nonEmpty) {
      offsetsEndMap ++ lowLimitOffsets
    } else {
      Map.empty[TopicPartition, Long]
    }

    val endingOffsets: Map[TopicPartition, Long] = if (lowLimitOffsets.nonEmpty) {
      offsetsEndMap ++ highLimitOffsets
    } else {
      Map.empty[TopicPartition, Long]
    }

    val numOffsets = startOffsets.keys.map(key => {
      endingOffsets(key)-startOffsets(key)
    }).sum

    TopicAndOffsets(topic, partitionOffsets(startOffsets), partitionOffsets(endingOffsets), endTime, numOffsets)
  }

  /**
    * Write per-TopicPartition offsets as json string
    */
  def partitionOffsets(partitionOffsets: Map[TopicPartition, Long]): Option[String] =
    if (partitionOffsets.nonEmpty) {
      val result = new mutable.HashMap[String, mutable.HashMap[Int, Long]]()
      implicit val ordering: Ordering[TopicPartition] = new Ordering[TopicPartition] {
        override def compare(x: TopicPartition, y: TopicPartition): Int =
          Ordering.Tuple2[String, Int].compare((x.topic, x.partition), (y.topic, y.partition))
      }
      val partitions = partitionOffsets.keySet.toSeq.sorted // sort for more determinism
      partitions.foreach { tp =>
        val off   = partitionOffsets(tp)
        val parts = result.getOrElse(tp.topic, new mutable.HashMap[Int, Long])
        parts += tp.partition -> off
        result += tp.topic    -> parts
      }
      Some(Serialization.write(result))
    } else {
      None
    }

  private def endOffsets(topic: String): Map[TopicPartition, Long] = {
    val partitionsInfo = consumer.partitionsFor(topic).asScala
    val partitions = partitionsInfo
      .map(partitionInfo => {
        new TopicPartition(partitionInfo.topic, partitionInfo.partition)
      })
      .asJava

    consumer
      .endOffsets(partitions)
      .asScala
      .toMap
      .asInstanceOf[Map[TopicPartition, Long]]

  }

  private def topicPartitionsWithTimestamps(topic: String,
                                                     timestamp: Long): Map[TopicPartition, Long] = {
    import scala.collection.JavaConverters._

    val partitionsInfo = consumer.partitionsFor(topic).asScala

    val timestampsToSearch: util.Map[TopicPartition, java.lang.Long] = partitionsInfo
      .map(partitionInfo => {
        new TopicPartition(partitionInfo.topic, partitionInfo.partition) -> timestamp
      })
      .toMap
      .asJava
      .asInstanceOf[util.Map[TopicPartition, java.lang.Long]]

    val offsetsForTimes: Map[TopicPartition, Long] = consumer
      .offsetsForTimes(timestampsToSearch)
      .asScala
      .filter { case (_, offsetAndTimestamp) => offsetAndTimestamp != null }
      .map {
        case (topicPartition, offsetAndTimestamp) =>
          (topicPartition, offsetAndTimestamp.offset())
      }
      .toMap

    offsetsForTimes
  }
}

object KafkaService {

  def apply(kafkaConfiguration: Config): KafkaService = {
    val props = new Properties()

    kafkaConfiguration.entrySet().asScala.foreach { entry =>
      val key = entry.getKey
      val value = entry.getValue.unwrapped().toString

      props.setProperty(key, value)
    }

    val consumer = new KafkaConsumer[String, String](props)

    new KafkaService(consumer, kafkaConfiguration)
  }
}
