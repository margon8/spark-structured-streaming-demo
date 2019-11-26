name := "spark-structured-streaming-demo"

organization := "eu.marcgonzalez"

version := "0.1"

scalaVersion := "2.11.12"

enablePlugins(PackagingTypePlugin)


val typesafeConfigVersion = "1.3.4"

val sparkVersion = "2.4.4"
val hadoopVersion = "2.8.4"
val awsJavaSdkVersion = "1.11.520"
val kafkaVersion = "2.0.0"

val scalatestVersion = "3.0.4"
val scalacheckVersion = "1.14.0"
val stPegdownVersion = "1.6.0"
val asmVersion = "5.2"
val dockerTestkitVersion = "0.9.9"
val playJsonVersion = "2.7.4"

val jackson2Version = "2.6.7"

val oldJersey = ExclusionRule(organization = "com.sun.jersey")
val oldJavax = ExclusionRule(organization = "javax.ws.rs")


libraryDependencies ++= Seq(

  //Utils
  "com.typesafe" % "config" % typesafeConfigVersion,

  // Spark on AWS Hadoop
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.hadoop" % "hadoop-aws" % hadoopVersion excludeAll(oldJersey, oldJavax),
  "com.amazonaws" % "aws-java-sdk" % awsJavaSdkVersion,
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "io.delta" %% "delta-core" % "0.4.0",


//Derby (local hive metastore)
  //"org.apache.derby" % "derby" % "10.15.1.3" % Test,

  // ScalaTest
  "org.scalatest" %% "scalatest" % scalatestVersion % Test,
  "org.pegdown" % "pegdown" % stPegdownVersion % Test,
  "org.ow2.asm" % "asm-all" % asmVersion % Test,

  // ScalaCheck
  "org.scalacheck" %% "scalacheck" % scalacheckVersion,

  //New Jersey
  "org.glassfish.jersey.core" % "jersey-client" % "2.22.2",

  // Docker
  "com.whisk" %% "docker-testkit-impl-docker-java" % dockerTestkitVersion % Test excludeAll(oldJersey, oldJavax),
  "com.whisk" %% "docker-testkit-scalatest" % dockerTestkitVersion % Test excludeAll(oldJersey, oldJavax),
  "com.whisk" %% "docker-testkit-config" % dockerTestkitVersion % Test excludeAll(oldJersey, oldJavax),

  //Jackson
   "org.json4s" %% "json4s-jackson" % "3.4.2"


  
)

dependencyOverrides ++=  Seq(
  "com.google.guava"           % "guava" % "16.0.1",
  "com.fasterxml.jackson.core" % "jackson-core" % jackson2Version,
  "com.fasterxml.jackson.core" % "jackson-databind" % jackson2Version,
  "com.fasterxml.jackson.core" % "jackson-annotations" % jackson2Version

)
