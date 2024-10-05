import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}

class TextInputFormatNoSplit extends TextInputFormat{
  override def isSplitable(context: JobContext, filename: Path): Boolean = false

}
