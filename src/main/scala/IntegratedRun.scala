import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.commons.io.FileUtils
import java.io.File

object IntegratedRun {

  // logger & general
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val appConf: Config = ConfigFactory.parseResources("application.conf").resolve()

  // directories
  val bigDataDir: String = appConf.getString("bigDataDir")
  val shardsDir: String = appConf.getString("shardsDir")
  val embeddingVectorDir = appConf.getString("embeddingVectorDir")
  val numTokenizerMapper: Int = appConf.getInt("numTokenizerMapper")
  val outputDir: String = appConf.getString("outputDir")
  val closestWordDir: String = appConf.getString("closestWordDir")

  def checkOutputDirExist(): Boolean = {
    val outputDirFile = new File(outputDir)
    outputDirFile.exists()
  }

  def main(args: Array[String]): Unit = {
    if (checkOutputDirExist()) {
      logger.error(s"Output directory $outputDir already exist! Exit app")
      return
    }

    // Split the Big Data into shards
    logger.info("Splitting Big Data")
    val SplitFileObj = new SplitGroupFiles(bigDataDir, shardsDir, numTokenizerMapper)
    SplitFileObj.main()

    // MapReduce Tokenize the shards to tokenID version
    logger.info("Tokenizing shards to TokenIds")
    val job1Status = TextTokenizerMR.main()
    if (job1Status == 1) {
      logger.error("Failed to tokenize shards to tokenIds")
      return
    }

    // MapReduce Vector embedding
    logger.info("Vector Embedding")
    val job2Status = VectorEmbeddingMR.main()
    if (job2Status == 0) {
      val file = new File(s"${embeddingVectorDir}part-r-00000")
      val csvFilePath = new File(file.getParent, "result.csv")
      val success = file.renameTo(csvFilePath)
      if (success) logger.info("Renaming hadoop default part-r-00000 to result.csv")
      logger.info(s"Vector embedding is in folder $embeddingVectorDir")
    }
    else {
      logger.error("Run failed")
      return
    }

    // Map Reduce ClosestWords
    logger.info("Getting all closest words")
    val job3Status = ClosestWordsMR.main()
    if (job3Status == 0) {
      logger.info(s"Successful Run! File detailing closest pairs is in folder $closestWordDir")
    }
    else {
      logger.error("Run failed")
    }
  }
}
