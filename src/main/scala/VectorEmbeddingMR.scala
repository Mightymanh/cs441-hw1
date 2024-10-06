import com.knuddels.jtokkit.Encodings
import com.knuddels.jtokkit.api.{Encoding, EncodingRegistry, EncodingType, IntArrayList}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.deeplearning4j.models.word2vec.VocabWord
import org.slf4j.{Logger, LoggerFactory}

import java.io.{File, FileWriter}
import scala.jdk.CollectionConverters._

/*
  Input: embeddingVectorDir
  Output: embeddingVectorDir
 */
object VectorEmbeddingMR {
  // logger & general
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val appConf: Config = ConfigFactory.load().resolve()

  // encoding
  val registry: EncodingRegistry = Encodings.newDefaultEncodingRegistry()
  val enc: Encoding = registry.getEncoding(EncodingType.CL100K_BASE)

  // Word2Vec hyper-parameters
  val seed: Int = appConf.getInt("seed")
  val numIterations: Int = appConf.getInt("numIterations")
  val numEpochs: Int = appConf.getInt("numEpochs")
  val windowSize: Int = appConf.getInt("windowSize")

  // Mapper: word, embedding is an array of double
  class EmbeddingMapper extends Mapper[LongWritable, Text, Text, DoubleArrayWritable] {
    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, DoubleArrayWritable]#Context): Unit = {
      val conf = context.getConfiguration
      val embeddingVectorSize = conf.get("embeddingVectorSize").toInt
      val minWordFrequency = conf.get("minWordFrequency").toInt

      // initialize model, input & output
      val inputPath = value.toString
      logger.info(s"Initialize Word Embedding Model. Input data: $inputPath")
      val embeddingModel = new VectorEmbeddingModel(inputPath, embeddingVectorSize, minWordFrequency, seed, numIterations, numEpochs, windowSize)

      // let the model consume whole file
      logger.info("Training data")
      embeddingModel.train()
      logger.info("Finish training")

      // get all word vectors and for each word vector write to context: key is word, and value is embedding
      val vocabCache = embeddingModel.model.getVocab
      val words = vocabCache.vocabWords()

      // key: real word, value: embedding vector, each is string representation of a double value
      words.asScala.foreach((word: VocabWord) => {
        val tokenIdStr: String = word.getWord
        val tokenIdIntArrayList = new IntArrayList()
        tokenIdIntArrayList.add(tokenIdStr.toInt)

        val realWord: String = enc.decode(tokenIdIntArrayList).trim()

        val freq = vocabCache.wordFrequency(tokenIdStr).toDouble
        val embeddingVector = embeddingModel.model.getWordVector(tokenIdStr)
        logger.debug(s"$realWord, freq: $freq, embedding vector: ${embeddingVector.mkString(" ")}")
        val bundleArr = Array.concat(Array(freq), embeddingVector)
        val obj = new DoubleArrayWritable(bundleArr)
        context.write(new Text(realWord), obj)
      })
    }
  }

  // Reducer
  class EmbeddingReducer extends Reducer[Text, DoubleArrayWritable, Text, Text] {
    override def reduce(key: Text, values: java.lang.Iterable[DoubleArrayWritable], context: Reducer[Text, DoubleArrayWritable, Text, Text]#Context): Unit = {

      // deconstruct the object
      val bundleArr = values.asScala.toArray.map((obj: DoubleArrayWritable) => obj.getDoubleArray)
      val vectorArr = bundleArr.map((arr: Array[Double]) => arr.tail)

      val freqArr = bundleArr.map((arr: Array[Double]) => arr.head)

      // average the vectors
      val numVectors = vectorArr.length
      val averageVector: Array[Double] = vectorArr.reduce((x, y) => {
        x.zip(y).map(x => x._1 + x._2)
      }).map(sum => sum / numVectors)

      // get frequency of given word
      val freq = freqArr.sum.toInt
      logger.debug(s"${key.toString}, freq: $freq, embedding vector: ${averageVector.mkString(" ")}")
      context.write(new Text(key.toString), new Text(s"$freq,${averageVector.mkString(",")}"))
    }
  }

  def submitJob(inputPath: String, outputPath: String, embeddingVectorSize: Int, minWordFrequency: Int): Boolean = {
    val jobConf: Configuration = new Configuration(true)
    val jobName = "Vector Embedding Map Reduce"
    jobConf.set("embeddingVectorSize", embeddingVectorSize.toString)
    jobConf.set("minWordFrequency", minWordFrequency.toString)

    // Job Configuration
    //    jobConf.set("fs.defaultFS", "file:///")
    jobConf.set("mapred.textoutputformat.separator", ",")
    //    jobConf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName);
    //    jobConf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName);

    // Initialize Job
    val job: Job = Job.getInstance(jobConf, jobName)
    job.setJarByClass(this.getClass)

    // Mapper
    job.setMapperClass(classOf[EmbeddingMapper])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[DoubleArrayWritable])

    // Reducer
    job.setNumReduceTasks(1)
    job.setReducerClass(classOf[EmbeddingReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    // Input & Output
    job.setInputFormatClass(classOf[TextInputFormatNoSplit])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])
    //    val jobInputPath = setUp()
    //    FileInputFormat.addInputPath(job, new Path(jobInputPath))
    FileInputFormat.addInputPath(job, new Path(inputPath))
    FileOutputFormat.setOutputPath(job, new Path(outputPath))

    // Summary of Setup Stats
    logger.info(s"Submit job: $jobName, input: $inputPath, output: $outputPath")
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
    val embeddingVectorSize = if (args.length >= 3) args(2).toInt else 100
    val minWordFrequency = if (args.length >= 4) args(3).toInt else 2
    submitJob(inputPath, outputPath, embeddingVectorSize, minWordFrequency)
  }
}