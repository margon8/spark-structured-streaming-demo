import infrastructure.kafka._
import infrastructure.test.BaseTest
import org.apache.spark.sql.DataFrame

class SumBatchTest extends BaseTest {

  import org.apache.spark.sql.functions._

  def selectKafkaContent(df: DataFrame): DataFrame =
    df.selectExpr("CAST(value AS STRING) as sValue")

  def jsonScore(df: DataFrame): DataFrame =
    df.selectExpr("CAST(get_json_object(sValue, '$.score') as INT) score")

  def sumScores(df: DataFrame): DataFrame =
    df.agg(sum("score").as("total"))

  it should "sum 51 after consuming everything" in {

    publishToMyKafka

    kafka.getTopics().size shouldBe 1

    val topicsAndOffsets = kafkaUtils.getTopicsAndOffsets("eu.marcgonzalez.demo")
    topicsAndOffsets.foreach { topicAndOffset: TopicAndOffsets =>
      val df = kafkaUtils
        .load(topicAndOffset, kafkaConfiguration)

      val jsonDf = df
        .transform(selectKafkaContent)
        .transform(jsonScore)
        .transform(sumScores)

      jsonDf.collect()(0).get(0) shouldBe 51

    }

  }

}
