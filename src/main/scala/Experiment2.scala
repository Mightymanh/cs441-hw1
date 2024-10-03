import com.knuddels.jtokkit.api.{Encoding, EncodingRegistry, EncodingType, IntArrayList}
import com.knuddels.jtokkit.Encodings
import com.typesafe.config.{Config, ConfigFactory}
import org.deeplearning4j.models.embeddings.learning.impl.elements.SkipGram
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer
import org.deeplearning4j.models.word2vec.{VocabWord, Word2Vec}

import java.io.File
import org.deeplearning4j.text.sentenceiterator.{LineSentenceIterator, SentenceIterator}
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.CommonPreprocessor
import org.deeplearning4j.text.tokenization.tokenizerfactory.{DefaultTokenizerFactory, TokenizerFactory}


object Experiment2 {
  val appConf: Config = ConfigFactory.parseResources("application.conf").resolve()

  val inputPath = appConf.getString("shardsDir")
  val outputPath = appConf.getString("tokenIdDir")
  val numMapper = 3
  val numReducer = 2
  
  def main(args: Array[String]): Unit = {
    val file = new File("/Users/mightymanh/Desktop/myCode/cs441/hw1/src/main/resources/output/embeddingVector/part-r-00000")
    val csvFilePath = new File(file.getParent, "result.txt")
    val success = file.renameTo(csvFilePath)
    println(success)
  }
}
