package demo.file

import infrastructure.kafka._
import infrastructure.test.BaseTest
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import org.joda.time.DateTime

class StreamDeltaTest extends BaseTest {

  import org.apache.spark.sql.functions._
  import testImplicits._

  val queryName = s"scores_${DateTime.now().getMillis}"

  def selectKafkaContent(df: DataFrame): DataFrame =
    df.selectExpr("CAST(value AS STRING) as sValue","timestamp")

  def jsonScore(df: DataFrame): DataFrame =
    df.selectExpr("CAST(get_json_object(sValue, '$.score') as INT) score")

  def jsonScoreAndDate(df: DataFrame): DataFrame =
    df.selectExpr("from_json(sValue, 'score INT, eventTime LONG, delayInMin INT') struct","timestamp as procTime")
      .select(col("struct.*"), 'procTime)
      .selectExpr("timestamp(eventTime/1000) as eventTime", "score", "procTime")


  def sumScores(df: DataFrame): DataFrame =
    df.agg(sum("score").as("total"))

  def windowedSumScores(df: DataFrame): DataFrame =
    df.groupBy(
      window($"eventTime", "2 minutes")
    ).agg(sum("score").as("total"))

  it should "write into delta" in {

    timelyPublishToMyKafka

    kafka.getTopics().size shouldBe 1
    kafka.offsetRangesByDatetime(
      kafka.getTopics().head,
      today.getMillis,
      today.withHourOfDay(1).getMillis
    ).numOffsets should be > 0L

    val topicsAndOffsets = kafkaUtils.getTopicsAndOffsets("eu.marcgonzalez.demo")
    topicsAndOffsets.foreach { topicAndOffset: TopicAndOffsets =>
      val df = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("startingOffsets", "earliest")
        .option("subscribe", topicAndOffset.topic)
        .load()

      df.isStreaming shouldBe true

      val jsonDf = df
        .transform(selectKafkaContent)
        .transform(jsonScoreAndDate)
        .withWatermark("eventTime", "2 minutes")
        .transform(windowedSumScores)

      val query = jsonDf.writeStream
        .outputMode("complete")
        .format("delta")
        .option("checkpointLocation", s"out/checkpoint/$queryName")
        .option("path", s"out/delta/$queryName")
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .start()

      query.awaitTermination(20 * SECONDS_MS)

      spark.read.format("delta").load(s"out/delta/$queryName").orderBy("window")
        .collect()
        .foldLeft(Seq.empty[Int])(
          (a, v) => a ++ Seq(v.get(1).asInstanceOf[Long].toInt)
        ) shouldBe Seq(14, 18, 4, 12)
      
    }

  }


}
