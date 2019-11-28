package demo.batch

import infrastructure.kafka._
import infrastructure.test.BaseTest
import org.apache.spark.sql.DataFrame

class BatchWindowedSumTest extends BaseTest {

  import org.apache.spark.sql.functions._
  import testImplicits._

  def selectKafkaContent(df: DataFrame): DataFrame =
    df.selectExpr("CAST(value AS STRING) as sValue", "timestamp")

  def jsonScoreAndDate(df: DataFrame): DataFrame =
    df.selectExpr(
        "from_json(sValue, 'score INT, eventTime LONG, delayInMin INT') struct",
        "timestamp as procTime"
      )
      .select(col("struct.*"), 'procTime)
      .selectExpr("timestamp(eventTime/1000) as eventTime", "score", "procTime")

  def parse(df: DataFrame): DataFrame = {
    jsonScoreAndDate(selectKafkaContent(df))
  }

  def sumScores(df: DataFrame): DataFrame =
    df.agg(sum("score").as("total"))

  def windowedSumScores(df: DataFrame): DataFrame =
    df.groupBy(window($"eventTime", "2 minutes")).agg(sum("score").as("total"))

  it should "sum 14, 18, 4, 12 after consuming everything in 2 minute windows" in {

    publishToMyKafka

    kafka.getTopics().size shouldBe 1

    val topicsAndOffsets =
      kafkaUtils.getTopicsAndOffsets("eu.marcgonzalez.demo")
    topicsAndOffsets.foreach { topicAndOffset: TopicAndOffsets =>
      val df = kafkaUtils
        .load(topicAndOffset, kafkaConfiguration)

      val jsonDf = df
        .transform(parse)
        .transform(windowedSumScores)

      jsonDf
        .sort("window")
        .collect()
        .foldLeft(Seq.empty[Int])(
          (a, v) => a ++ Seq(v.get(1).asInstanceOf[Long].toInt)
        ) shouldBe Seq(14, 18, 4, 12)

    }

  }

}
