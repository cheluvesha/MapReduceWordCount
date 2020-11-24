package com.WordCount

import java.io.IOException

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{MapReduceBase, Mapper, OutputCollector, Reporter}
import org.apache.log4j.Logger

/***
 * WordCountMapper -  Mapper Class Which Implements Map task
 */
class WordCountMapper
    extends MapReduceBase
    with Mapper[LongWritable, Text, Text, IntWritable] {
  val  logger: Logger = Logger.getLogger(getClass.getName)
  private final val one: IntWritable = new IntWritable(1)
  private val word = new Text()

  /***
   * Map function Implements Map task
   * @param key LongWritable
   * @param value Text
   * @param output OutputCollector[Text, IntWritable]
   * @param reporter Reporter
   * @throws IOException
   */
  @throws[IOException]
  def map(
      key: LongWritable,
      value: Text,
      output: OutputCollector[Text, IntWritable],
      reporter: Reporter
  ): Unit = {
    logger.info("Inside Mapper Function")
    try {
    val line: String = value.toString
    line.split(" ").foreach { token =>
      word.set(token)
      logger.info(word+" ->   "+one)
      output.collect(word, one)
    }
  }
    catch {
      case ex: Exception =>
       println(ex.getMessage)
    }
  }
}
