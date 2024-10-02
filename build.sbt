import scala.collection.Seq

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.5.0"

lazy val root = (project in file("."))
  .settings(
    name := "cs441-hw1"
  )

// https://mvnrepository.com/artifact/ch.qos.logback/logback-classic
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.5.8" % Test

// https://mvnrepository.com/artifact/com.typesafe/config
libraryDependencies += "com.typesafe" % "config" % "1.4.3"

// https://mvnrepository.com/artifact/com.knuddels/jtokkit
libraryDependencies += "com.knuddels" % "jtokkit" % "1.0.0"

// hadoop
libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "3.4.0",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.4.0",
  "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "3.4.0"
)
excludeDependencies += "org.slf4j" % "slf4j-reload4j" // by default it includes in hadoop

// https://mvnrepository.com/artifact/org.deeplearning4j/deeplearning4j-core
libraryDependencies += "org.deeplearning4j" % "deeplearning4j-core" % "1.0.0-M2.1"
// https://mvnrepository.com/artifact/org.deeplearning4j/deeplearning4j-nlp
libraryDependencies += "org.deeplearning4j" % "deeplearning4j-nlp" % "1.0.0-M2.1"
// https://mvnrepository.com/artifact/org.nd4j/nd4j-native-platform
libraryDependencies += "org.nd4j" % "nd4j-native-platform" % "1.0.0-M2.1"

