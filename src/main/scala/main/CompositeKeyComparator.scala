package main

import org.apache.hadoop.io.{Text, WritableComparable, WritableComparator}

class CompositeKeyComparator extends WritableComparator(classOf[Text], true) {
  @SuppressWarnings(Array("rawtypes"))
  override def compare(wc1: WritableComparable[?], wc2: WritableComparable[?]): Int = {
    val key1 = wc1.asInstanceOf[Text]
    val key2 = wc2.asInstanceOf[Text]
    val str1 = key1.toString
    val str2 = key2.toString
//    println("Inside Comparator")
    val sepPos1 = str1.indexOf('#')
    val sepPos2 = str2.indexOf('#')
    val lineOff1 = str1.substring(0, sepPos1).toInt
    val file1 = str1.substring(sepPos1 + 1)
    val lineOff2 = str2.substring(0, sepPos2).toInt
    val file2 = str2.substring(sepPos2 + 1)
//    println(s"$file1 $lineOff1")
//    println(s"$file2 $lineOff2")
//    println("Done Comparator")
    if (file1 == file2) then lineOff1.compareTo(lineOff2)
    else file1.compareTo(file2)
  }
}
