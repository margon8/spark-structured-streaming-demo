import infrastructure.kafka._
import infrastructure.test.BaseTest
import org.apache.spark.sql.DataFrame

class WindowedSumBatchTest extends BaseTest {

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


  def sumScores(df: DataFrame): DataFrame =
    df.agg(sum("score").as("total"))

  def windowedSumScores(df: DataFrame): DataFrame =
    df.groupBy(
      window($"eventTime", "2 minutes")
    ).agg(sum("score").as("total"))

  it should "sum 14,22,3,12 after consuming everything inw 2 minute windows" in {

    publishToMyKafka

    kafka.getTopics().size shouldBe 1

    val topicsAndOffsets = kafkaUtils.getTopicsAndOffsets("eu.marcgonzalez.demo")
    topicsAndOffsets.foreach { topicAndOffset: TopicAndOffsets =>
      val df = kafkaUtils
        .load(topicAndOffset, kafkaConfiguration)

      val jsonDf = df
        .transform(selectKafkaContent)
        .transform(jsonScoreAndDate)

      jsonDf.printSchema()
      jsonDf.show(false)

      jsonDf.transform(windowedSumScores)
        .take(4).foldLeft(Seq.empty[Int])((a,v) => a ++ Seq(v.get(1).asInstanceOf[Long].toInt)) shouldBe Seq(14,22,3,12)

    }

  }

}
