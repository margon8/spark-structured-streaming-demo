package infrastructure.kafka

case class TopicAndOffsets(topic: String,
                           startingOffsetsAsJson: Option[String],
                           endingOffsetsAsJson: Option[String],
                           endTime: Long,
                           numOffsets: Long)
