import org.apache.hadoop.mapreduce.Partitioner
import org.apache.hadoop.io.Text


class CompositeKeyPartitioner extends Partitioner[Text, Text] {
  override def getPartition(key: Text, value: Text, numPartitions: Int): Int = {
    // Automatic n-partitioning using hash on the state name
    val str = key.toString
    val sepPos = str.indexOf('#')
    val lineOff = str.substring(0, sepPos).toInt
    val file = str.substring(sepPos + 1)
//    println(s"Inside Partitioner: $numPartitions, file: $file")
    Math.abs(file.hashCode & Integer.MAX_VALUE) % numPartitions
  }
}
