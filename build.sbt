name := "spark-structured-streaming-demo"

organization := "eu.marcgonzalez"

version := "0.1"

scalaVersion := "2.11.12"

val typesafeConfigVersion = "1.3.4"

val sparkVersion = "2.4.4"
val hadoopVersion = "2.8.4"
val awsJavaSdkVersion = "1.11.520"
val kafkaVersion = "2.0.0"

val scalatestVersion = "3.0.4"
val scalacheckVersion = "1.14.0"
val stPegdownVersion = "1.6.0"
val asmVersion = "5.2"
val dockerTestkitVersion = "0.9.8"
val playJsonVersion = "2.7.4"


libraryDependencies ++= Seq(

  //Utils
  "com.typesafe" % "config" % typesafeConfigVersion,

  // Spark on AWS Hadoop
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.hadoop" % "hadoop-aws" % hadoopVersion,
  "com.amazonaws" % "aws-java-sdk" % awsJavaSdkVersion,
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,

  // ScalaTest
  "org.scalatest" %% "scalatest" % scalatestVersion % Test,
  "org.pegdown" % "pegdown" % stPegdownVersion % Test,
  "org.ow2.asm" % "asm-all" % asmVersion % Test,

  // ScalaCheck
  "org.scalacheck" %% "scalacheck" % scalacheckVersion,

  // Docker
  "com.whisk" %% "docker-testkit-impl-docker-java" % dockerTestkitVersion % Test,
  "com.whisk" %% "docker-testkit-scalatest" % dockerTestkitVersion % Test,
  "com.whisk" %% "docker-testkit-config" % dockerTestkitVersion % Test


)