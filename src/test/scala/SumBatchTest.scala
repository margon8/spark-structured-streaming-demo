import infrastructure.docker.DockerIntegrationTest
import infrastructure.kafka.{DomainEvent, KafkaDockerService, KafkaEvent}
import org.joda.time.DateTime
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._
import org.json4s.NoTypeHints
import org.scalatest.{FunSpec, Matchers}


class SumBatchTest extends FunSpec with Matchers{
  //extends DockerIntegrationTest with KafkaDockerService {

  describe("A Kafka with some events") {

    //publishToMyKafka

    it(" should return one topic") {
      true shouldBe true
    }
  }

  def test(): Unit = {

  }

  private def publishToMyKafka= {

    val today = DateTime.now.withTimeAtStartOfDay()

    val events = Seq(
      DomainEvent(5, today.withMinuteOfHour(0), 5 ),
      DomainEvent(9, today.withMinuteOfHour(1), 7 ),
      DomainEvent(7, today.withMinuteOfHour(2), 3 ),
      DomainEvent(8, today.withMinuteOfHour(3), 4 ),
      DomainEvent(3, today.withMinuteOfHour(3), 3 ),
      DomainEvent(4, today.withMinuteOfHour(3), 2 ),
      DomainEvent(3, today.withMinuteOfHour(4), 2 ),
      DomainEvent(3, today.withMinuteOfHour(6), 1 ),
      DomainEvent(8, today.withMinuteOfHour(7), 1 ),
      DomainEvent(1, today.withMinuteOfHour(7), 1 )
    )

    implicit val formats = Serialization.formats(NoTypeHints)

    val kafkaEvents =
      events.map(
        event =>
          KafkaEvent(
            "eu.marcgonzalez.demo",
            write(event),
            event.eventTime.plusMinutes(event.delayInMin).getMillis
          )
      )

    //publishToKafka(kafkaEvents)

  }

}
