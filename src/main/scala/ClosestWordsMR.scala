import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}

import java.io.{BufferedReader, InputStreamReader}
import org.apache.hadoop.mapreduce.{Job, Mapper}
import org.slf4j.{Logger, LoggerFactory}

/*
  inputPath: embedding vector csv
  outputPath: closestWordsDir
 */
object ClosestWordsMR {

  // logger & general
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val appConf: Config = ConfigFactory.load().resolve()

  // configurations
//  val embeddingCsv: String = appConf.getString("embeddingCsv")
//  val closestWordDir: String = appConf.getString("closestWordDir")
  val numMapper: Int = 1
  val numReducer: Int = 1

  def dotProduct(v1: Array[Double], v2: Array[Double]): Double = {
    v1.zip(v2).map((x) => {
      val x1: Double = x._1
      val x2: Double = x._2
      x1 * x2
    }).sum
  }

  def cosine(v1: Array[Double], v2: Array[Double]): Double = {
    val dotProd = dotProduct(v1, v2)
    val length1 = Math.sqrt(dotProduct(v1, v1))
    val length2 = Math.sqrt(dotProduct(v2, v2))
    Math.abs(dotProd) / (length1 * length2)
  }

  def findClosestWord(currWord: String, currVector: Array[Double], csvFilePath: String): String = {
    val conf = new Configuration()

    val path = new Path(csvFilePath)
    val fs = path.getFileSystem(conf)
    val inputStream: FSDataInputStream = fs.open(path)
    val buffer = new BufferedReader(new InputStreamReader(inputStream))

    // find the closest word. I have to use var because it works well with buffer, expecially if the csv file is hugh with 35000 words and
    // if we use map, we have to store 35000 embedding vectors in memory which is costly
    var closestWord: String = ""
    var highestCos: Double = 0
    buffer.lines().forEach(line => {
      val fields = line.split(",").map(_.trim)
      if (!fields.isEmpty) { // deal with empty line at the end
        val word = if (fields.head == "") "," else fields.head
        if (word != currWord) { // deal with repeated word
          val vector = if (fields.head == "") fields.tail.tail.tail.map(_.toDouble) else fields.tail.tail.map(_.toDouble)
          val cos = cosine(currVector, vector)
          logger.debug(s"$currWord vs $word: score: ${cos.toString}, ${vector.mkString(",")}")
          if (cos > highestCos) {
            highestCos = cos
            closestWord = word
          }
        }
      }
    })

    // close properly
    buffer.close()
    inputStream.close()
//    fs.close()

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
      val conf = context.getConfiguration
      val embeddingCsv = conf.get("embeddingCsv")

      val currLine: String = value.toString
      val currFields = currLine.split(",").map(_.trim)
      if (currFields.isEmpty) return // deal with empty line at the end

      val currWord = if (currFields.head == "") "," else currFields.head
      val currVector = if (currFields.head == "") currFields.tail.tail.tail.map(_.toDouble) else currFields.tail.tail.map(_.toDouble)

      logger.debug(s"$currWord: ${currVector.mkString(",")}")
      // get the closest word
      val closestWord = findClosestWord(currWord, currVector, embeddingCsv)
      context.write(new Text(currWord), new Text(closestWord))
    }
  }

  def submitJob(inputPath: String, outputPath: String): Boolean = {
    // configure job
    val jobConf: Configuration = new Configuration(true)
    jobConf.set("embeddingCsv", inputPath)
    val jobName = "Closest Words Map Reduce"

    // Job Configuration
    //    jobConf.set("fs.defaultFS", "file:///")
//    jobConf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName);
//    jobConf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName);

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
    FileInputFormat.addInputPath(job, new Path(inputPath))
    FileOutputFormat.setOutputPath(job, new Path(outputPath))

    // Summary of Setup Stats
    logger.info("Submit job: {}, with #mappers: {}, #reducers: {}", jobName, numMapper, numReducer)
    logger.info("Input: {}, Output: {}", inputPath, outputPath)
    logger.info(s"Mapper class: ${job.getMapperClass}, Reducer class: ${job.getReducerClass}")


    // Run job
    if (job.waitForCompletion(true)) {
      logger.info("Success")
      true
    }
    else {
      logger.info("Failed")
      false
    }
  }

  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val outputPath = args(1)
    submitJob(inputPath, outputPath)
  }
}


