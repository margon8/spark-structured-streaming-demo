package infrastructure.kafka

case class KafkaEvent(topic: String, event: String, timestamp: Long) extends Serializable
