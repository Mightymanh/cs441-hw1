package main

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import scala.io.Source

object ClosestWordsMR {

  // logger & general
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val appConf: Config = ConfigFactory.load().resolve()

  // configurations
  val embeddingCsv: String = appConf.getString("embeddingCsv")
  val closestWordDir: String = appConf.getString("closestWordDir")
  val numMapper: Int = 1
  val numReducer: Int = 1

  def dotProduct(v1: Array[Double], v2: Array[Double]): Double = {
    v1.zip(v2).map((x1, x2) => x1 * x2).sum
  }

  def cosine(v1: Array[Double], v2: Array[Double]): Double = {
    val dotProd = dotProduct(v1, v2)
    val length1 = Math.sqrt(dotProduct(v1, v1))
    val length2 = Math.sqrt(dotProduct(v2, v2))
    Math.abs(dotProd) / (length1 * length2)
  }

  def findClosestWord(currWord: String, currVector: Array[Double]): String = {
    val csvFile = new File(embeddingCsv)
    val buffer = Source.fromFile(csvFile)

    // find the longest word
    // I have to use var here, because im reading a very long file
    var closestWord: String = ""
    var highestCos: Double = 0
    buffer.getLines().foreach(line => {
      val fields = line.split(",").map(_.trim)
      if (!fields.isEmpty) { // deal with empty line at the end
        val word = if (fields.head == "") "," else fields.head
        if (word != currWord) { // deal with repeated word
          val vector = if (fields.head == "") fields.tail.tail.tail.map(_.toDouble) else fields.tail.tail.map(_.toDouble)
          val cos = cosine(currVector, vector)
          logger.info(s"$word: ${vector.mkString(",")}")
          logger.info(cos.toString)
          if (cos > highestCos) {
            highestCos = cos
            closestWord = word
          }
        }
      }
    })
    buffer.close()

    closestWord
  }

  // Mapper
  class ClosestWordMapper extends Mapper[LongWritable, Text, Text, Text] {
    override def setup(context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
      val fileName = (context.getInputSplit).asInstanceOf[FileSplit].getPath.getName
      logger.info(s"Mapper is processing file: $fileName")
    }

    @throws[Exception]
    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
      val currLine: String = value.toString
      val currFields = currLine.split(",").map(_.trim)
      if (currFields.isEmpty) return // deal with empty line at the end

      val currWord = if (currFields.head == "") "," else currFields.head
      val currVector = if (currFields.head == "") currFields.tail.tail.tail.map(_.toDouble) else currFields.tail.tail.map(_.toDouble)

      logger.info(s"$currWord: ${currVector.mkString(",")}")
      // get the closest word
      val closestWord = findClosestWord(currWord, currVector)
      context.write(new Text(currWord), new Text(closestWord))
    }
  }


  def main(): Int = {
    // configure job
    val jobConf: Configuration = new Configuration(true)
    val jobName = "Closest Words Map Reduce"

    // Job Configuration
    jobConf.set("fs.defaultFS", "file:///")
    jobConf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName);
    jobConf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName);

    // Initialize Job
    val job: Job = Job.getInstance(jobConf, jobName)
    job.setJarByClass(this.getClass)

    // Mapper
    job.setMapperClass(classOf[ClosestWordMapper])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Text])

    // Reducer
    job.setNumReduceTasks(1)
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    // Input & Output
    job.setInputFormatClass(classOf[TextInputFormatNoSplit])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])
    FileInputFormat.addInputPath(job, new Path(embeddingCsv))
    FileOutputFormat.setOutputPath(job, new Path(closestWordDir))

    // Summary of Setup Stats
    logger.info("Submit job: {}, with #mappers: {}, #reducers: {}", jobName, numMapper, numReducer)
    logger.info("Input: {}, Output: {}", embeddingCsv, closestWordDir)
    println(job.getMapperClass)
    println(job.getReducerClass)

    // Run job
    if (job.waitForCompletion(true)) {
      logger.info("Success")
      0
    }
    else {
      logger.info("Failed")
      1
    }
  }
}


