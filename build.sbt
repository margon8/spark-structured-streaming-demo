name := "spark-structured-streaming-demo"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.4"
val hadoopVersion = "2.8.4"
val awsJavaSdkVersion = "1.11.520"
val kafkaVersion = "2.0.0"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.hadoop" % "hadoop-aws" % hadoopVersion,
  "com.amazonaws" % "aws-java-sdk" % awsJavaSdkVersion,
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion
)