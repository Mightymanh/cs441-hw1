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
  val appConf: Config = ConfigFactory.parseResources("application.conf")

  val inputPath = appConf.getString("shardsDir")
  val outputPath = appConf.getString("tokenIdDir")
  val numMapper = 3
  val numReducer = 2
  
  def main(args: Array[String]): Unit = {
//    val obj = new TextTokenizerMR(inputPath, outputPath, numMapper, numReducer)
//    obj.main()
  }
}
