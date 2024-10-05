import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}

object IntegratedRun {
  // logger & general
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val appConf: Config = ConfigFactory.load().resolve()

  def prepareEmbeddingInput(tokenIdDir: String, inputEmbeddingDir: String): Unit = {
    val conf = new Configuration()
    val fs: FileSystem = FileSystem.get(conf)
    val fileStatuses = fs.listStatus(new Path(tokenIdDir))
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

  def checkDirExist(outputPath: String): Boolean = {
    val conf = new Configuration()
    val fs: FileSystem = FileSystem.get(conf)
    val outputDirFile = new Path(outputPath)
    val exist = fs.exists(outputDirFile)
    fs.close()
    exist
  }

  def createEmbeddingCsv(embeddingVectorDir: String, embeddingCsv: String): Boolean = {
    val conf = new Configuration()
    val fs: FileSystem = FileSystem.get(conf)
    val fromPath = new Path(embeddingVectorDir, "part-r-00000")
    val destPath = new Path(embeddingCsv)
    val success = fs.rename(fromPath, destPath)
    fs.close()
    success
  }

  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val outputPath = args(1)

    if (checkDirExist(outputPath)) {
      logger.error(s"Output directory already exists: $outputPath")
      return
    }

    // preparing directories
    val tokenIdDir = new Path(outputPath, appConf.getString("tokenIdDir")).toString
    val inputEmbeddingDir = new Path(outputPath, appConf.getString("inputEmbeddingDir")).toString
    val embeddingVectorDir = new Path(outputPath, appConf.getString("embeddingVectorDir")).toString
    val embeddingCsv = new Path(outputPath, appConf.getString("embeddingCsv")).toString
    val closestWordDir = new Path(outputPath, appConf.getString("closestWordDir")).toString

    // Tokenize
    logger.info("Start Map Reduce tokenize")
    val tokenizeStatus = TextTokenizerMR.submitJob(inputPath, tokenIdDir, 2)
    if (!tokenizeStatus) {
      logger.error("Tokenizing text failed")
      return
    }

    // Preparing input for embedding
    logger.info("Preparing inputs for Vector embedding")
    prepareEmbeddingInput(tokenIdDir, inputEmbeddingDir)

    // Embedding
    logger.info("Start Map Reduce Vector embedding")
    val embeddingStatus = VectorEmbeddingMR.submitJob(inputEmbeddingDir, embeddingVectorDir)
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
