# CS 441 Fall 2024 - homework 1
Author: Manh Phan

Email: dphan8@uic.edu

Youtube demo Deploying app in AWS: 

# Instruction on running this homework

Open this project at your favorite IDEA, my ideal IDEA is IntelliJ. But a terminal is also fine to run this homework.
The project is run using sbt command. Ensure you have Java 11 and Scala 2.13 installed before running this project.

To run this project: At the root of this project run this command:

Compile project: `sbt compile`

## The main class file for this project is IntegratedRun.scala. 
**IntegratedRun** has the following in order jobs:
- TextTokenizer Map Reduce: shards of text files -> tokenID version
- VectorEmbedding Map Reduce: tokenId -> vector embedding file
- ClosestWord Map Reduce: from vector embedding file (result.csv) -> exporting file that has list of pairs with closest semantic (cosine) similarity

IntegratedRun receives the following input in order:
- inputPath: the path to the shards directory
- outputPath: the path to directory where all computational results are stored
- numReducer (optional, default = 2): number of reducers for the tokenize map reduce task
- embeddingVectorSize (optional, default = 100): the size of embedding vector for vector embedding map reduce
- minWordFrequency (optional, default = 2): the min word frequency to learn for vector embedding map reduce

So run: `sbt run <inputPath> <outputPath> <numReducer (optional)> <embeddingVectorSize (optional)> <minWordFrequency (optional)>`

then when IDEA or terminal ask which program you want to run, choose IntegratedRun.

## To split a big dataset into shards, run the **SplitGroupFiles.scala**: 
`sbt run <inputBigDataDir> <shardsDir> <numGroups>`
- inputBigDataDir: path to your big data directory
- shardsDir: path to directory where you will store your shards
- numGroups: how many shards do you want (the num shards here are relative number)

## Additional commands
`sbt clean`: clean project

`sbt test`: run test. There are 5 tests in the **src/test/** directory

## About dataset to train word2vec
When processing a big dataset, the design of the dataset is very important. Some people put all text into a single file. My dataset is a directory containing lots of text files and each text file is a book. The dataset is from [https://www.gutenberg.org/](https://www.gutenberg.org/)

