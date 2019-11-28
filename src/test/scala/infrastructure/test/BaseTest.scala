package infrastructure.test
import com.typesafe.config.ConfigFactory
import infrastructure.docker.DockerIntegrationTest
import infrastructure.kafka._
import org.joda.time.DateTime
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

abstract class BaseTest extends DockerIntegrationTest with KafkaDockerService with SharedSparkSessionHelper {

  val kafkaConfiguration = ConfigFactory.load().getConfig("kafka")
  val kafka = KafkaService(kafkaConfiguration)

  val random = new scala.util.Random
  val MINUTES_MS = 60000
  val SECONDS_MS = 1000


  val kafkaUtils = new KafkaUtils(
    spark,
    DateTime.now.withTimeAtStartOfDay(),
    DateTime.now.withTimeAtStartOfDay().withHourOfDay(1),
    kafka
  )

  // TODO: Move to configuration file.
  val TopicsParalellism     = 50
  val DefaultTimeoutInSec   = 3600
  val DefaultCoolDownInSec  = 60
  val MaxEventsPerPartition = 100000

  val today = DateTime.now.withTimeAtStartOfDay()

  private def genEvents(withEOF: Boolean = false): Seq[KafkaEvent] = {

    val events = Seq(
      DomainEvent(5, today.withMinuteOfHour(0).getMillis, today.withMinuteOfHour(5).getMillis),
      DomainEvent(9, today.withMinuteOfHour(1).getMillis, today.withMinuteOfHour(8).getMillis),
      DomainEvent(7, today.withMinuteOfHour(2).getMillis, today.withMinuteOfHour(5).getMillis),
      DomainEvent(8, today.withMinuteOfHour(3).getMillis, today.withMinuteOfHour(7).getMillis),
      DomainEvent(3, today.withMinuteOfHour(3).getMillis, today.withMinuteOfHour(6).getMillis),
      DomainEvent(4, today.withMinuteOfHour(4).getMillis, today.withMinuteOfHour(6).getMillis),
      DomainEvent(3, today.withMinuteOfHour(6).getMillis, today.withMinuteOfHour(7).getMillis),
      DomainEvent(8, today.withMinuteOfHour(7).getMillis, today.withMinuteOfHour(8).getMillis),
      DomainEvent(1, today.withMinuteOfHour(7).getMillis, today.withMinuteOfHour(9).getMillis),
      DomainEvent(0, today.withMinuteOfHour(10).getMillis, today.withMinuteOfHour(11).getMillis)
    )

    implicit val formats = Serialization.formats(NoTypeHints)

    val kafkaEvents =
      events
        .filter(_.score > 0)
        .map(event =>
          KafkaEvent(
            "eu.marcgonzalez.demo",
            write(event),
            event.procTime + random.nextInt(100)
          )
      ).sortBy(f => f.timestamp)

    kafkaEvents
  }

  def publishToMyKafka = publishToKafka(genEvents())

  def timelyPublishToMyKafka = {
    timedPublishToKafka(genEvents())
    kafkaSemaphore
  }

  def timelyPublishToMyKafkaWithEof = {
    timedPublishToKafka(genEvents(true))
    kafkaSemaphore
  }

  private def kafkaSemaphore = {
    while (kafka.getTopics().isEmpty
           || kafka
             .offsetRangesByDatetime(
               kafka.getTopics().head,
               today.getMillis,
               today.withHourOfDay(1).getMillis
             )
             .numOffsets == 0) {
      Thread.sleep(100)
    }
  }
}
