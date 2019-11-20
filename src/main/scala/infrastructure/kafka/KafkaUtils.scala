package infrastructure.kafka

import java.sql.Timestamp
import java.time.{Instant, ZoneId}

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.joda.time.DateTime

import collection.JavaConverters._

object KafkaUtils {
  // TODO: Move to configuration file.
  val TopicsParalellism     = 50
  val DefaultTimeoutInSec   = 3600
  val DefaultCoolDownInSec  = 60
  val MaxEventsPerPartition = 100000

}

class KafkaUtils(spark: SparkSession,
                      startDateTime: DateTime, currentDateTime: DateTime,
                      kafkaService: KafkaService) extends LazyLogging {

    def getTopicsAndOffsets(topics: String*): Seq[TopicAndOffsets] = {
    val startTime: Long = startDateTime.toInstant().getMillis
    val endTime: Long   = currentDateTime.toInstant().getMillis

    topics
      .map(topic => {

        kafkaService.offsetRangesByDatetime(topic, startTime, endTime)
      })
      .filter {
        case TopicAndOffsets(topic, startingOffsets, endingOffsets, _, _) =>
          val hasElements = startingOffsets.isDefined && endingOffsets.isDefined

          if (!hasElements) {
            logger.info("Topic {} has not offsets to process.", topic)
          }
          hasElements
      }
  }

  def load(topicAndOffsets: TopicAndOffsets, kafkaConfiguration: Config): DataFrame = {
    logger.info(
      "Processing topic: {}, startingOffsets: {}, endingOffsets: {}",
      topicAndOffsets.topic,
      topicAndOffsets.startingOffsetsAsJson.get,
      topicAndOffsets.endingOffsetsAsJson.get
    )
    val numPartitions: Long =
      if (topicAndOffsets.numOffsets >= KafkaUtils.MaxEventsPerPartition) {
        topicAndOffsets.numOffsets / KafkaUtils.MaxEventsPerPartition
      } else { 1 }

    generateKafkaReader(
      inputStream(topicAndOffsets.topic,
        topicAndOffsets.startingOffsetsAsJson.get,
        topicAndOffsets.endingOffsetsAsJson.get,
        kafkaConfiguration))
      .where(
        col("timestamp") < Timestamp.valueOf(
          Instant
            .ofEpochMilli(topicAndOffsets.endTime)
            .atZone(ZoneId.systemDefault())
            .toLocalDateTime))
      .coalesce(numPartitions.toInt)
  }

  private def inputStream(topic: String,
                          startingOffsets: String,
                          endingOffsets: String,
                          kafkaConfiguration: Config): DataFrameReader = {

    val properties = kafkaConfiguration.entrySet()
      .asScala
      .map { entry =>
        val value = entry.getValue.unwrapped() match {
          case collection: java.util.Collection[_] =>
            collection.asScala.mkString(",")
          case string: String => string
          case other: Any     => other.toString
        }
        entry.getKey -> value
      }
      .toMap

    spark.read
      .format("kafka")
      .option("subscribe", topic)
      .option("startingOffsets", startingOffsets)
      .option("endingOffsets", endingOffsets)
      .options(properties)
  }

  private def generateKafkaReader(reader: DataFrameReader): DataFrame =
    reader.load()
}
