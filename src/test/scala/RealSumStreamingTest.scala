import infrastructure.kafka._
import infrastructure.test.BaseTest
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger

class RealSumStreamingTest extends BaseTest {

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

  it should "sum 14,22,3,12 after streaming everything in 2 minute windows" in {

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
        .transform(selectKafkaContent)
        .transform(jsonScoreAndDate)
        .transform(windowedSumScores)

      val query = jsonDf.writeStream
        .outputMode("complete") //complete
        .format("memory") //console
        .queryName("scores")
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .start()

      query.awaitTermination(10 * 1000)

      spark.sql("select * from scores order by window asc")
        .take(4)
        .foldLeft(Seq.empty[Int])(
          (a, v) => a ++ Seq(v.get(1).asInstanceOf[Long].toInt)
        ) shouldBe Seq(14, 22, 3, 12)
      
    }

  }


}

/*def createWriter(df: Dataset[Row], topic: String, triggerIntervalInSec: Int,): DataStreamWriter[Row] =
  df.writeStream
    .queryName("kafka_" + topic)
    .trigger(Trigger.ProcessingTime(triggerIntervalInSec + " seconds"))
    .option("checkpointLocation", checkpoint + topic)
    .format("text")
    .option("path", outPath + topic)
    .outputMode("append")

def startQuery(writer: DataStreamWriter[Row]): StreamingQuery =
  writer.start

def inputStream(topic: String): DataStreamReader = {
  val kafkaUrl = config.getOrElse("kafkaUrl", "")
  spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaUrl)
    .option("failOnDataLoss", value = false)
    .option("startingOffsets", "earliest")
    .option("subscribe", topic)
}

private def generateKafkaReader(reader: DataStreamReader): DataFrame =
  reader.load()

}*/
