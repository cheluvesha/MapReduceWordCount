package com.WordCount

import java.io.IOException
import java.util

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.{MapReduceBase, OutputCollector, Reducer, Reporter}

class WordCountReducer
    extends MapReduceBase
    with Reducer[Text, IntWritable, Text, IntWritable] {

  @throws[IOException]
  def reduce(
      key: Text,
      values: util.Iterator[IntWritable],
      output: OutputCollector[Text, IntWritable],
      reporter: Reporter
  ) {
    import scala.collection.JavaConversions._
    val sum = values.toList.reduce((valueOne, valueTwo) =>
      new IntWritable(valueOne.get() + valueTwo.get())
    )

    output.collect(key, new IntWritable(sum.get()))
  }
}
