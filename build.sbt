//import scala.collection.Seq

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "cs441-hw1"
  )

// https://mvnrepository.com/artifact/ch.qos.logback/logback-classic
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.13"
//libraryDependencies += ""
// https://mvnrepository.com/artifact/com.typesafe/config
libraryDependencies += "com.typesafe" % "config" % "1.4.3"

// https://mvnrepository.com/artifact/com.knuddels/jtokkit
libraryDependencies += "com.knuddels" % "jtokkit" % "1.0.0"

// hadoop
libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "3.3.6",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.3.6",
  "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "3.3.6"
)
excludeDependencies += "org.slf4j" % "slf4j-reload4j" // by default it includes in hadoop
excludeDependencies += "log4j" % "log4j"
excludeDependencies += "org.slf4j" % "slf4j-log4j12"

// https://mvnrepository.com/artifact/org.deeplearning4j/deeplearning4j-core
libraryDependencies += "org.deeplearning4j" % "deeplearning4j-core" % "1.0.0-M2.1"
// https://mvnrepository.com/artifact/org.deeplearning4j/deeplearning4j-nlp
libraryDependencies += "org.deeplearning4j" % "deeplearning4j-nlp" % "1.0.0-M2.1"
// https://mvnrepository.com/artifact/org.nd4j/nd4j-native-platform
libraryDependencies += "org.nd4j" % "nd4j-native-platform" % "1.0.0-M2.1"

// https://mvnrepository.com/artifact/org.scalameta/munit
libraryDependencies += "org.scalameta" %% "munit" % "1.0.2" % Test

// META-INF discarding
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) =>
    xs match {
      case "MANIFEST.MF" :: Nil => MergeStrategy.discard
      case "services" :: _ => MergeStrategy.concat
      case _ => MergeStrategy.discard
    }
  case "reference.conf" => MergeStrategy.concat
  case x if x.endsWith(".proto") => MergeStrategy.rename
  case x if x.contains("hadoop") => MergeStrategy.first
  case _ => MergeStrategy.first
}