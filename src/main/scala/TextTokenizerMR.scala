import com.knuddels.jtokkit.Encodings
import com.knuddels.jtokkit.api.{Encoding, EncodingRegistry, EncodingType, IntArrayList}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.slf4j.{Logger, LoggerFactory}

import java.lang
import scala.jdk.CollectionConverters._

/*
  Input: shardsDir (hdfs), numReducer
  Output: TokenIdsDir (hdfs)
 */
object TextTokenizerMR {

  // logger & general
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  // encoding
  val registry: EncodingRegistry = Encodings.newDefaultEncodingRegistry()
  val enc: Encoding = registry.getEncoding(EncodingType.CL100K_BASE)

  class TokenIdMapper extends Mapper[LongWritable, Text, Text, Text] {

    override def setup(context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
      val fileName = (context.getInputSplit).asInstanceOf[FileSplit].getPath.getName
      logger.info(s"Mapper is processing file: $fileName")
    }

    @throws[Exception]
    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
      val line: String = value.toString
      val encoded: IntArrayList = enc.encode(line)
      if (encoded.isEmpty) return
      val fileName = (context.getInputSplit).asInstanceOf[FileSplit].getPath.getName
      val outputKey = key.get().toString + "#" + fileName
      context.write(new Text(outputKey), new Text(encoded.toArray.mkString(" ")))
    }
  }

  class TokenIdReducer extends Reducer[Text, Text, NullWritable, Text] {
    @throws[Exception]
    override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, NullWritable, Text]#Context): Unit = {
      values.asScala.foreach((value: Text) => {
        context.write(NullWritable.get(), new Text(value.toString))
      })
    }
  }

  def submitJob(inputPath: String, outputPath: String, numReducer: Int = 2): Boolean = {
    val jobConf: Configuration = new Configuration(true)
    val jobName = "Tokenizer Map Reduce"

    // Job Configuration
    //    jobConf.set("fs.defaultFS", "file:///")
    //    jobConf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName);
    //    jobConf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName);

    // Initialize Job
    val job: Job = Job.getInstance(jobConf, jobName)
    job.setJarByClass(this.getClass)

    // Mapper
    job.setMapperClass(classOf[TokenIdMapper])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Text])

    // Middleware
    job.setSortComparatorClass(classOf[CompositeKeyComparator])
    job.setPartitionerClass(classOf[CompositeKeyPartitioner])

    // Reducer
    job.setNumReduceTasks(numReducer)
    job.setReducerClass(classOf[TokenIdReducer])
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[Text])

    // Input & Output
    job.setInputFormatClass(classOf[TextInputFormatNoSplit])
    job.setOutputFormatClass(classOf[TextOutputFormat[NullWritable, Text]])
    FileInputFormat.addInputPath(job, new Path(inputPath))
    FileOutputFormat.setOutputPath(job, new Path(outputPath))

    // Summary of Setup Stats
    logger.info("Submit job: {}, with #reducers: {}", jobName, numReducer)
    logger.info("Input: {}, Output: {}", inputPath, outputPath)
    logger.info(s"Mapper class: ${job.getMapperClass}, Reducer class: ${job.getReducerClass}")

    // Run job
    if (job.waitForCompletion(true)) {
      logger.info("Success")
      true
    }
    else {
      logger.info("Failed")
      false
    }
  }

  def main(args: Array[String]): Unit = {

    val inputPath: String = args(0)
    val outputPath: String = args(1)
    val numReducer: Int = args(2).toInt

    submitJob(inputPath, outputPath, numReducer)
  }
}

