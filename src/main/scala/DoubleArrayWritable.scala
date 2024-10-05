import org.apache.hadoop.io.{ArrayWritable, DoubleWritable}

// Define the DoubleArrayWritable class
class DoubleArrayWritable extends ArrayWritable(classOf[DoubleWritable]) {

  // Constructor that initializes the array of DoubleWritable
  def this(values: Array[Double]) = {
    this() // Calls the default constructor
    set(values.map(new DoubleWritable(_))) // Convert array of Double to DoubleWritable
  }

  // A method to get the values as an Array[Double]
  def getDoubleArray: Array[Double] = {
    get().map(_.asInstanceOf[DoubleWritable].get)
  }

  // Override toString for a better representation
  override def toString: String = getDoubleArray.mkString("[", ", ", "]")
}