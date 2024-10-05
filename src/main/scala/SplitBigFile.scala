import scala.io.Source
import java.io.{File, PrintWriter}

object SplitBigFile {

  // Function to split large file into smaller files
  def splitFile(filePath: String, linesPerFile: Int): Unit = {
    val source = Source.fromFile(filePath)

    try {
      val lines = source.getLines().grouped(linesPerFile)
      lines.zipWithIndex.foreach { case (chunk, index) =>
        val chunkFileName = s"${filePath}_part_$index.txt"
        writeToFile(chunkFileName, chunk)
      }
    } finally {
      source.close()
    }
  }

  // Pure function to write each chunk to a new file
  def writeToFile(fileName: String, lines: Seq[String]): Unit = {
    val writer = new PrintWriter(new File(fileName))

    try {
      lines.foreach(writer.println)
    } finally {
      writer.close()
    }
  }

  def main(args: Array[String]): Unit = {
    // Example: split large file into files with 1000 lines each
    val largeFilePath = "largefile.txt"
    splitFile(largeFilePath, 1)
  }
}
