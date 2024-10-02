import org.slf4j.{LoggerFactory, Logger}

import java.io.File
import java.io.FileWriter
import scala.annotation.tailrec
import org.apache.commons.io.FileUtils
import scala.io.Source
import scala.io.Codec
import java.nio.charset.CodingErrorAction
import com.typesafe.config.{Config, ConfigFactory}

/*
  Input: bigDataDir
  Output: shardDir
 */
class SplitFile(dataDirPath: String, shardsDir: String, numMapper: Int) {
  
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  
  def getListOfFiles(dir: String): List[String] = {
    val file = new File(dir)
    file.listFiles.filter((file: File) => { // only allow .txt file, ignore other trash file like DS_Store
      if (!file.isFile) false
      else {
        val name = file.getName
        if (name.length > 4 && name.substring(name.length -4) == ".txt") true
        else false
      }
    }).map(_.getPath).toList
  }

  // merge files
  def mergeFiles(files: List[File], destFile: File): Unit = {
    logger.info(s"Merging files ${files.map((file: File) => file.getName).mkString(", ")} to new file: ${destFile.getPath}")
    val writer = new FileWriter(destFile, true)
    @tailrec
    def _mergeFiles(files: List[File]): Unit = {
      if (files.isEmpty) writer.close()
      else {
        val currentFile = files.head
//        val decoder = Charset.forName("UTF-8").newDecoder()
//        decoder.onMalformedInput(CodingErrorAction.IGNORE)
        implicit val codec: Codec = Codec("UTF-8")
        codec.onMalformedInput(CodingErrorAction.IGNORE) // need this to ignore MalformedInput
        val buffer = Source.fromFile(currentFile)
        buffer.getLines().foreach(line => {
          writer.write(line + "\n")
        })
        buffer.close()
        _mergeFiles(files.tail)
      }
    }
    _mergeFiles(files)
    logger.info("Finish merging")
  }

  def splitToShards(fileList: List[File], limit: Long): Int = {
    @tailrec
    def _splitToShards(fileList: List[File], tempList: List[File], currentSize: Long, count: Int): Int = {
      if (fileList.isEmpty) { // if we finish processing list then merge all files in temp
        if (tempList.nonEmpty) {
          logger.debug(s"Last Shard: $count")
          val newFile = new File(shardsDir + count.toString + ".txt")
          mergeFiles(tempList, newFile)
          count + 1
        }
        else count
      }
      else { // if we havent finish processing list
        if (currentSize > limit) { // if the temp list surpassed limit, merge them
          logger.debug(s"Shard $count: current size pass threshold")
          val newFile = new File(shardsDir + count.toString + ".txt")
          mergeFiles(tempList, newFile)
          _splitToShards(fileList, List(), 0, count + 1)
        }
        else { // if the temp list still has available spot, continue collect file
          val currentFile = fileList.head
          val currentFileSize = currentFile.length()
          if (currentFileSize > limit) { // if the current file surpass limit, export it
            logger.debug(s"Shard $count: this file is big: $currentFileSize vs limit:$limit")
            val newFile = new File(shardsDir + count.toString + ".txt")
            mergeFiles(List(currentFile), newFile)
            _splitToShards(fileList.tail, tempList, currentSize, count + 1)
          }
          else { // continue collecting
            val newTempList = currentFile +: tempList
            val newSize = currentSize + currentFile.length()
            _splitToShards(fileList.tail, newTempList, newSize, count)
          }
        }
      }
    }
    _splitToShards(fileList, List(), 0, 0)
  }

  def main(): Unit = {
    val filePathList = getListOfFiles(dataDirPath)
    val FileList = filePathList.map((filePath: String) => new File(filePath))
    val shardLimitSize = FileList.map((file: File) => file.length()).sum / numMapper
    logger.info(s"Processing ${filePathList.length} files with approximately $numMapper mappers, threshold of shard size is $shardLimitSize")

    // set up shards folder if it does not exist
    val shardsDirFile: File = new File(shardsDir)
    if (shardsDirFile.exists()) {
      FileUtils.deleteDirectory(shardsDirFile)
      logger.warn("Shards dir already exists! Deleted the directory")
    }
    val successful = shardsDirFile.mkdirs()
    if (successful) {
      logger.info("Shards directory is created successfully")
    }
    else {
      logger.error("Shards directory failed to create")
      System.exit(1)
    }

    // split big data into shards
    logger.info("Start splitting to Shards")
    val numShards = splitToShards(FileList, shardLimitSize)
    logger.info(s"Finish splitting to $numShards Shards, output to $shardsDir")
  }
}
