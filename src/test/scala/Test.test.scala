import com.knuddels.jtokkit.Encodings
import com.knuddels.jtokkit.api.{Encoding, EncodingRegistry, EncodingType, IntArrayList}
import org.apache.commons.io.FileUtils

import java.io.File
import org.apache.hadoop.fs.Path

import scala.io.Source


class Test extends munit.FunSuite {

  def readFile(file: String): String = {
    val buffer = Source.fromFile(file)
    val lines = buffer.mkString
    buffer.close()
    lines
  }

  def cleanUp() = {
    val dir: File = new File(outputTestPath)
    if (dir.exists()) {
      FileUtils.deleteDirectory(dir)
    }
  }

  override def beforeEach(context: BeforeEach): Unit = {
    println(s"setting up test ${context.test.name}")
    cleanUp()
  }
  override def afterEach(context: AfterEach): Unit = {
    cleanUp()
    println(s"closing up test ${context.test.name}")
  }

  override def afterAll(): Unit = {
    cleanUp()
  }

  val outputTestPath = "src/test/resources/testOutput/"

  // encoding
  val registry: EncodingRegistry = Encodings.newDefaultEncodingRegistry()
  val enc: Encoding = registry.getEncoding(EncodingType.CL100K_BASE)

  // test if sharding algorithm is as expect: split the files into relatively equal size groups
  test("Sharding") {
    val inputPath = "src/test/resources/testInput/shardingTest"
    val outputPath = "src/test/resources/testOutput/shards"
    val numGroups = 1
    println(inputPath)
    println(outputPath)
    // check sharding runs fine
    val status = SplitGroupFiles.splitJob(inputPath, outputPath, numGroups)
    assertEquals(status, true, "Sharding job fails")

    // check number of shards created
    val expectedNumGroups = 1
    val actualNumGroups = new File(outputPath).listFiles.length
    assertEquals(expectedNumGroups, actualNumGroups, "Incorrect number of shards")
  }

  // check accuracy of tokenizing map reduce: the output is the encoded version of input
  test("Tokenizing") {
    cleanUp()

    val inputPath = "src/test/resources/testInput/tokenizingTest"
    val outputPath = "src/test/resources/testOutput/tokenId"
    val numReducer = 1

    val status = TextTokenizerMR.submitJob(inputPath, outputPath, numReducer)
    assertEquals(status, true, "Tokenizing job fails")

    // check the file
    val line1 = enc.encode("health health banana banana exit exit").toArray.mkString(" ")
    val line2 = enc.encode("eat thinking thinking thinking salute salute salute window").toArray.mkString(" ")
    val expectedContent1 = line1 + "\n" + line2 + "\n"
    val expectedContent2 = line2 + "\n" + line1 + "\n"

    // check file exist
    val filePath = new Path(outputPath, "part-r-00000").toString
    assertEquals(new File(filePath).exists(), true, "Tokenizing job fails")

    val actualContent = readFile(filePath)
    assertEquals((actualContent == expectedContent2) || (actualContent == expectedContent1), true, "Tokenize wrong, incorrect order of words")
  }

  // test cosine between two vectors
  test("cosine") {
    val v1 = Array(0.5, 0.2, 0.3)
    val v2 = Array(0.3, 0.4, 0.1)
    val expectedResult = 0.82717
    val actualResult = ClosestWordsMR.cosine(v1, v2)
    assertEquals(Math.abs(expectedResult - actualResult) < 0.001, true, "cosine between two vectors is wrong")
  }

  // test cosine similarity
  test("closestWord") {
    val embeddingCsv = "src/test/resources/testInput/closestWordTest/embedding.csv"
    val word = "girl"
    val vector = Array(0.1, 0.2, 0.3)
    assertEquals(ClosestWordsMR.findClosestWord(word, vector, embeddingCsv), "woman", "closest word is wrong")
  }

  // test cosine similarity and special case where there are only two words in the csv file
  test("closestWord2") {
    val embeddingCsv = "src/test/resources/testInput/closestWordTest/embedding.csv"
    val word = "boy"
    val vector = Array(0.21, 0.5, 0.11)
    assertEquals(ClosestWordsMR.findClosestWord(word, vector, embeddingCsv), "man", "closest word is wrong")

    val embeddingCsv2 = "src/test/resources/testInput/closestWordTest/embedding2.csv"
    val word2 = "man"
    val vector2 = Array(0.5,0.2,0.1,0.9)
    assertEquals(ClosestWordsMR.findClosestWord(word2, vector2, embeddingCsv2), "woman", "closest word is wrong")
  }

}
