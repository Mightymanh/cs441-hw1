import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}

object IntegratedRun {
  // logger & general
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val appConf: Config = ConfigFactory.load().resolve()

  // prepare input for vector embedding map reduce: for each output file from tokenize MR, export a file that has similar name and content is
  // the absolute path of the output file
  def prepareEmbeddingInput(tokenIdDir: String, inputEmbeddingDir: String): Unit = {
    val conf = new Configuration()
    val path = new Path(tokenIdDir)
    val fs: FileSystem = path.getFileSystem(conf)
    val fileStatuses = fs.listStatus(path)
    val wantedFiles = fileStatuses.map(status => status.getPath).filter((path) => {
      val name = path.getName
      if (name.length > 7 && name.substring(0, 7) == "part-r-") true
      else false
    })

    wantedFiles.foreach((file: Path) => {
      val newFile = new Path(inputEmbeddingDir, file.getName)
      val ostream: FSDataOutputStream = fs.create(newFile)
      ostream.writeBytes(file.toString)
      ostream.close()
    })
    fs.close()
  }

  // check if the output directory exist
  def checkDirExist(outputPath: String): Boolean = {
    val conf = new Configuration()
    val outputDirFile = new Path(outputPath)
    val fs = outputDirFile.getFileSystem(conf)
    val exist = fs.exists(outputDirFile)
    fs.close()
    exist
  }

  // convert output of embedding map reduce from "part-r-00000" to name in csv extension
  def createEmbeddingCsv(embeddingVectorDir: String, embeddingCsv: String): Boolean = {
    val conf = new Configuration()
    val fromPath = new Path(embeddingVectorDir, "part-r-00000")
    val destPath = new Path(embeddingCsv)
    val fs = fromPath.getFileSystem(conf)
    val success = fs.rename(fromPath, destPath)
    fs.close()
    success
  }

  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val outputPath = args(1)
    val numTokenReducer = if (args.length >= 3) args(2).toInt else 2
    val embeddingVectorSize = if (args.length >= 4) args(3).toInt else 100
    val minWordFrequency = if (args.length >= 5) args(4).toInt else 2
    logger.info(s"shard folders: $inputPath -> output folder: $outputPath")
    logger.info(s"numTokenReducer: $numTokenReducer, embeddingVectorSize: $embeddingVectorSize, minWordFreq: $minWordFrequency")

//    if (checkDirExist(outputPath)) {
//      logger.error(s"Output directory already exists: $outputPath")
//      return
//    }
    val conf = new Configuration()
    logger.info(conf.get("fs.defaultFS"))

    // preparing directories
    val tokenIdDir = new Path(outputPath, appConf.getString("tokenIdDir")).toString
    val inputEmbeddingDir = new Path(outputPath, appConf.getString("inputEmbeddingDir")).toString
    val embeddingVectorDir = new Path(outputPath, appConf.getString("embeddingVectorDir")).toString
    val embeddingCsv = new Path(outputPath, appConf.getString("embeddingCsv")).toString
    val closestWordDir = new Path(outputPath, appConf.getString("closestWordDir")).toString

    // Tokenize
    logger.info(s"Start Map Reduce tokenize: $numTokenReducer")
    val tokenizeStatus = TextTokenizerMR.submitJob(inputPath, tokenIdDir, numTokenReducer)
    if (!tokenizeStatus) {
      logger.error("Tokenizing text failed")
      return
    }

    // Preparing input for embedding
    logger.info("Preparing inputs for Vector embedding")
    prepareEmbeddingInput(tokenIdDir, inputEmbeddingDir)

    // Embedding
    logger.info(s"Start Map Reduce Vector embedding: vector size: $embeddingVectorSize, min word freq: $minWordFrequency")
    val embeddingStatus = VectorEmbeddingMR.submitJob(inputEmbeddingDir, embeddingVectorDir, embeddingVectorSize, minWordFrequency)
    if (!embeddingStatus) {
      logger.error("Vector embedding failed")
      return
    }

    // converting result to csv file
    val csvExist = createEmbeddingCsv(embeddingVectorDir, embeddingCsv)
    if (!csvExist) {
      logger.error("Failed to create vector embedding csv file")
      return
    }

    // Closest word
    logger.info("Start Map Reduce Closest Word")
    val closestStatus = ClosestWordsMR.submitJob(embeddingCsv, closestWordDir)
    if (!closestStatus) {
      logger.error("Getting closest words failed")
      return
    }

    logger.info(s"Finished! All results are in $outputPath")

  }
}
