package main

import com.knuddels.jtokkit.Encodings
import com.knuddels.jtokkit.api.{Encoding, EncodingRegistry, EncodingType, IntArrayList}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, ObjectWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.deeplearning4j.models.embeddings.learning.impl.elements.SkipGram
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer
import org.deeplearning4j.models.word2vec.{VocabWord, Word2Vec}
import org.deeplearning4j.text.sentenceiterator.{LineSentenceIterator, SentenceIterator}
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.CommonPreprocessor
import org.deeplearning4j.text.tokenization.tokenizerfactory.{DefaultTokenizerFactory, TokenizerFactory}
import org.slf4j.{Logger, LoggerFactory}

import java.io.{File, FileWriter}
import scala.jdk.CollectionConverters.*

/*
  Input: tokenIdDir
  Output: token
 */
object VectorEmbeddingMR {
  // logger & general
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val appConf: Config = ConfigFactory.load().resolve()

  // encoding
  val registry: EncodingRegistry = Encodings.newDefaultEncodingRegistry()
  val enc: Encoding = registry.getEncoding(EncodingType.CL100K_BASE)

  // folders
  val inputPath: String = appConf.getString("tokenIdDir")
  val outputPath: String = appConf.getString("embeddingVectorDir")
  val numReducer: Int = 1

  // Word2Vec hyperparameters
  val minWordFrequency: Int = appConf.getInt("minWordFrequency")
  val seed: Int = appConf.getInt("seed")
  val numIterations: Int = appConf.getInt("numIterations")
  val numEpochs: Int = appConf.getInt("numEpochs")
  val embeddingVectorSize: Int = appConf.getInt("embeddingVectorSize")
  val windowSize: Int = appConf.getInt("windowSize")

  // for each file starting with part-r, create a new file with the same name in txt, and inside contains the actual path
  def setUp(): String = {
    // get all files that start with "part-r-"
    val dir = new File(inputPath)
    val fileList = dir.listFiles.filter((file: File) => { // only allow "part-r-..." file, ignore other trash file like DS_Store
      if (!file.isFile) false
      else {
        val name = file.getName
        if (name.length > 7 && name.substring(0, 7) == "part-r-") true
        else false
      }
    })
    logger.info(s"Getting ${fileList.length} tokenId files: ${fileList.map(_.getName).mkString("Array(", ", ", ")")}")

    // create a new folder inside tokenId that will store all files with content filepath
    val jobDirPath = inputPath + "inputForEmbedding/"
    val jobDir = new File(jobDirPath)
    if (jobDir.exists()) {
      FileUtils.deleteDirectory(jobDir)
    }
    val successful = jobDir.mkdirs()
    if (successful) {
      logger.info(s"Created successfully folder: $jobDirPath")
    }
    else {
      logger.error(s"Failed to create folder: $jobDirPath")
      System.exit(1)
    }

    // populate files in that job dir
    fileList.foreach((file: File) => {
      val filePath = file.getPath
      val fileName = file.getName
      val newFile = new File(s"$jobDirPath/$fileName")
      val writer = new FileWriter(newFile)
      writer.write(filePath)
      writer.close()
    })
    logger.info("Finish creating temporary files in the inputForEmbedding folder. These files are input to Vector Embedding Map Reduce.")
    jobDirPath
  }

  // Mapper: word, embedding is an array of double
  class EmbeddingMapper extends Mapper[LongWritable, Text, Text, DoubleArrayWritable] {
    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, DoubleArrayWritable]#Context): Unit = {
      // initialize model, input & output
      val inputPath = value.toString
      val embeddingModel = new VectorEmbeddingModel(inputPath, embeddingVectorSize, minWordFrequency, seed, numIterations, numEpochs, windowSize)
      logger.info(s"Initialize Word Embedding Model. Input data: $inputPath")

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
        x.zip(y).map((a, b) => a + b)
      }).map(sum => sum / numVectors)

      // get frequency of given word
      val freq = freqArr.sum.toInt
      logger.info(s"${key.toString}, freq: $freq, embedding vector: ${averageVector.mkString(" ")}")
      context.write(new Text(key.toString), new Text(s"$freq,${averageVector.mkString(",")}"))
    }
  }

  def main(): Int = {
    
    val jobConf: Configuration = new Configuration(true)
    val jobName = "Vector Embedding Map Reduce"

    // Job Configuration
//    jobConf.set("fs.defaultFS", "file:///")
    jobConf.set("mapred.textoutputformat.separator", ",")
    jobConf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName);
    jobConf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName);

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
    val jobInputPath = setUp()
    FileInputFormat.addInputPath(job, new Path(jobInputPath))
    FileOutputFormat.setOutputPath(job, new Path(outputPath))

    // Summary of Setup Stats
    val numMapper = new File(jobInputPath).listFiles.length
    logger.info("Submit job: {}, with #mappers: {}, #reducers: {}", jobName, numMapper, numReducer)
    logger.info("Input: {}, Output: {}", inputPath, outputPath)
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


class VectorEmbeddingModel(inputFilePath: String, embeddingVectorSize: Int, minWordFrequency: Int, seed: Int, numIterations: Int, numEpochs: Int, windowSize: Int) {

  // logger
//  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  // model configuration
  println(inputFilePath)
  val file: File = new File(inputFilePath)
  val iter: SentenceIterator = new LineSentenceIterator(file)
  val tokenizerFactory: TokenizerFactory = new DefaultTokenizerFactory()

  // define model
  val model: Word2Vec = new Word2Vec.Builder()
    .minWordFrequency(minWordFrequency) // minimum frequency of words to be included
    .iterations(numIterations) // number of training iterations
    .epochs(numEpochs)
    .layerSize(embeddingVectorSize) // size of the word vectors
    .seed(seed)
    .windowSize(windowSize) // context window size for embedding
    .elementsLearningAlgorithm(new SkipGram[VocabWord]())
    .iterate(iter)
    .tokenizerFactory(tokenizerFactory)
    .build()

  def train(): Unit = {
    model.fit()
  }

  def saveModel(path: String): Unit = {
    WordVectorSerializer.writeWord2VecModel(model, new File(path))
  }
}
