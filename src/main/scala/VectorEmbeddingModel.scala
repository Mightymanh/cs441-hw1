import org.deeplearning4j.models.embeddings.learning.impl.elements.SkipGram
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer
import org.deeplearning4j.models.word2vec.{VocabWord, Word2Vec}
import org.deeplearning4j.text.sentenceiterator.{BasicLineIterator, CollectionSentenceIterator, LineSentenceIterator, SentenceIterator}
import org.deeplearning4j.text.tokenization.tokenizerfactory.{DefaultTokenizerFactory, TokenizerFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}

import java.io.{BufferedInputStream, File, InputStream, BufferedReader, InputStreamReader}
import java.util
import scala.jdk.CollectionConverters._


class VectorEmbeddingModel(inputFilePath: String, embeddingVectorSize: Int, minWordFrequency: Int, seed: Int, numIterations: Int, numEpochs: Int, windowSize: Int) {

  // logger & general
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  // model configuration
  val conf = new Configuration()
  logger.info(s"fs: ${conf.get("fs.defaultFS")}, input: $inputFilePath")
  val fs: FileSystem = FileSystem.get(conf)

  val sentences: util.Collection[String] = readFile(inputFilePath)
  val iter: SentenceIterator = new CollectionSentenceIterator(sentences)
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

  // read file and get list of lines
  def readFile(inputFilePath: String): util.Collection[String] = {
    val path = new Path(inputFilePath)
    val inputStream: FSDataInputStream = fs.open(path)
    val buffer = new BufferedReader(new InputStreamReader(inputStream))
    val arr = buffer.lines().iterator().asScala.toList.asJava
    buffer.close()
    inputStream.close()
    arr
  }

  def train(): Unit = {
    model.fit()
  }

  def saveModel(path: String): Unit = {
    WordVectorSerializer.writeWord2VecModel(model, new File(path))
  }
}
