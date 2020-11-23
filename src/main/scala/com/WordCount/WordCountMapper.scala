package com.WordCount

import java.io.IOException

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{MapReduceBase, Mapper, OutputCollector, Reporter}

class WordCountMapper
    extends MapReduceBase
    with Mapper[LongWritable, Text, Text, IntWritable] {
  private final val one: IntWritable = new IntWritable(1)
  private val word = new Text()

  @throws[IOException]
  def map(
      key: LongWritable,
      value: Text,
      output: OutputCollector[Text, IntWritable],
      reporter: Reporter
  ) {
    val line: String = value.toString
    line.split(" ").foreach { token =>
      word.set(token)

      output.collect(word, one)
    }
  }
}
