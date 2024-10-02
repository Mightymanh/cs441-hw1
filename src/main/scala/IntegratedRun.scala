import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.commons.io.FileUtils
import java.io.File

object IntegratedRun {

  // logger & general
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val appConf: Config = ConfigFactory.parseResources("application.conf")

  // directories
  val bigDataDir: String = appConf.getString("bigDataDir")
  val shardsDir: String = appConf.getString("shardsDir")
  val embeddingVectorDir = appConf.getString("embeddingVectorDir")
  val numTokenizerMapper: Int = appConf.getInt("numTokenizerMapper")
  val outputDir: String = appConf.getString("outputDir")
  def checkOutputDirExist(): Boolean = {
    val outputDirFile = new File(outputDir)
    if (outputDirFile.exists()) {
      true
    }
    else false
  }

  def main(args: Array[String]): Unit = {
    if (checkOutputDirExist()) {
      logger.error(s"Output directory $outputDir already exist! Exit app")
      return
    }

    logger.info("Splitting Big Data")
    // Split the Big Data into shards
    val SplitFileObj = new SplitFile(bigDataDir, shardsDir, numTokenizerMapper)
    SplitFileObj.main()

    logger.info("Tokenizing shards to TokenIds")
    // MapReduce Tokenize the shards to tokenID version
    val job1Status = TextTokenizerMR.main()
    if (job1Status == 1) {
      logger.error("Failed to tokenize shards to tokenIds")
      return
    }

    logger.info("Vector Embedding")
    // MapReduce Vector embedding
    val job2Status = VectorEmbeddingMR.main()
    if (job2Status == 0) {
      logger.info(s"Run successfully. Vector embedding is in folder $embeddingVectorDir")
    }
    else {
      logger.error("Run failed")
    }
  }
}
