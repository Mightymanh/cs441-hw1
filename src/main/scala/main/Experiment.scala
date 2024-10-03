package main

import com.knuddels.jtokkit.Encodings
import com.knuddels.jtokkit.api.{Encoding, EncodingRegistry, EncodingType, IntArrayList}
import org.deeplearning4j.models.embeddings.learning.impl.elements.SkipGram
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer
import org.deeplearning4j.models.word2vec.{VocabWord, Word2Vec}
import org.deeplearning4j.text.sentenceiterator.{LineSentenceIterator, SentenceIterator}
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.CommonPreprocessor
import org.deeplearning4j.text.tokenization.tokenizerfactory.{DefaultTokenizerFactory, TokenizerFactory}

import java.io.File

object Experiment {

  val registry: EncodingRegistry = Encodings.newDefaultEncodingRegistry()
  val enc: Encoding = registry.getEncoding(EncodingType.CL100K_BASE)

  def main(args: Array[String]): Unit = {
    val file: File = new File("/Users/mightymanh/Desktop/myCode/cs441/hw1/src/main/resources/input/input.txt")
    val iter: SentenceIterator = new LineSentenceIterator(file)
    val tokenizerFactory: TokenizerFactory = new DefaultTokenizerFactory()

    println("Defining model")
    // defining model
    val word2vec: Word2Vec = new Word2Vec.Builder()
      .minWordFrequency(1) // minimum frequency of words to be included
      .iterations(10) // number of training iterations
      .epochs(3)
      .layerSize(100) // size of the word vectors
      .seed(42)
      .windowSize(30) // context window size for embedding
      .elementsLearningAlgorithm(new SkipGram[VocabWord]())
      .iterate(iter)
      .tokenizerFactory(tokenizerFactory)
      .build()


    println("Start training the model")
    // training
    word2vec.fit()

    println("Done training the mode, now save it")
    // Save the model for later use
    WordVectorSerializer.writeWord2VecModel(word2vec, new File("/Users/mightymanh/Desktop/myCode/cs441/hw1/src/main/resources/output/embeddingVector/word2vec_model.bin"));
    println("Testing the model")
    // Print embedding
    // Get embedding for a token
    val embedding: Array[Double] = word2vec.getWordVector("584") // Replace "example" with your token
    if (embedding != null) {
      println("Embedding for 'health': ")
      for (value <- embedding) {
        print(s"$value ")
      }
    }
    else println("Word not in the vocabulary!")
    val vocabCache = word2vec.getVocab
    val freq = vocabCache.wordFrequency("584")
    println(vocabCache.words.toArray.mkString(" "))
    println(freq)

  }
}
