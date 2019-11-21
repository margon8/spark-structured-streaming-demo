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


  def publishToMyKafka = {

    val today = DateTime.now.withTimeAtStartOfDay()

    val events = Seq(
      DomainEvent(5, today.withMinuteOfHour(0).getMillis, 5),
      DomainEvent(9, today.withMinuteOfHour(1).getMillis, 7),
      DomainEvent(7, today.withMinuteOfHour(2).getMillis, 3),
      DomainEvent(8, today.withMinuteOfHour(3).getMillis, 4),
      DomainEvent(3, today.withMinuteOfHour(3).getMillis, 3),
      DomainEvent(4, today.withMinuteOfHour(3).getMillis, 2),
      DomainEvent(3, today.withMinuteOfHour(4).getMillis, 2),
      DomainEvent(3, today.withMinuteOfHour(6).getMillis, 1),
      DomainEvent(8, today.withMinuteOfHour(7).getMillis, 1),
      DomainEvent(1, today.withMinuteOfHour(7).getMillis, 1)
    )

    implicit val formats = Serialization.formats(NoTypeHints)

    val kafkaEvents =
      events.map(
        event =>
          KafkaEvent(
            "eu.marcgonzalez.demo",
            write(event),
            event.eventTime + event.delayInMin * MINUTES_MS + random.nextInt(100)
          )
      ).sortBy(f => f.timestamp)

    publishToKafka(kafkaEvents)

  }

}
