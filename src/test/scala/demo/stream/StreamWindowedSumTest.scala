package demo.stream

import infrastructure.kafka._
import infrastructure.test.BaseTest
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import org.joda.time.DateTime

class StreamWindowedSumTest extends BaseTest {

  val queryName = s"scores_${DateTime.now().getMillis}"

  import org.apache.spark.sql.functions._
  import testImplicits._

  def selectKafkaContent(df: DataFrame): DataFrame =
    df.selectExpr("CAST(value AS STRING) as sValue","timestamp")

  def jsonScore(df: DataFrame): DataFrame =
    df.selectExpr("CAST(get_json_object(sValue, '$.score') as INT) score")

  def jsonScoreAndDate(df: DataFrame): DataFrame =
    df.selectExpr("from_json(sValue, 'score INT, eventTime LONG, delayInMin INT') struct","timestamp as procTime")
      .select(col("struct.*"), 'procTime)
      .selectExpr("timestamp(eventTime/1000) as eventTime", "score", "procTime")

  def parse(df: DataFrame): DataFrame = {
    jsonScoreAndDate(selectKafkaContent(df))
  }

  def sumScores(df: DataFrame): DataFrame =
    df.agg(sum("score").as("total"))

  def windowedSumScores(df: DataFrame): DataFrame =
    df.groupBy(
      window($"eventTime", "2 minutes").as("window")
    ).agg(sum("score").as("total"))

  it should "sum 14,18,4,12 after streaming everything in 2 minute windows" in {

    publishToMyKafka

    kafka.getTopics().size shouldBe 1

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
        .transform(parse)
        .transform(windowedSumScores)

      val query = jsonDf.writeStream
        .outputMode("update") //complete
        .format("memory") //console
        .queryName(queryName)
        .trigger(Trigger.ProcessingTime("5 seconds")) //Once
        .start()

      query.awaitTermination(10 * 1000)

      spark.sql(s"select * from $queryName order by window asc")
        .collect()
        .foldLeft(Seq.empty[Int])(
          (a, v) => a ++ Seq(v.get(1).asInstanceOf[Long].toInt)
        ) shouldBe Seq(14, 18, 4, 12)

    }

  }

}

