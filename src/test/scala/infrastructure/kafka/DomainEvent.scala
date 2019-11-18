package infrastructure.kafka

import org.joda.time.DateTime

case class DomainEvent (score: Int, eventTime: DateTime, delayInMin: Int)