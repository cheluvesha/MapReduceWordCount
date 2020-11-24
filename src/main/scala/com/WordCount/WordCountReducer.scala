package com.WordCount

import java.io.IOException
import java.util
import java.util.logging.Logger

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.{MapReduceBase, OutputCollector, Reducer, Reporter}

/***
 * WordCountReducer Which Implements Reduce Task
 */
class WordCountReducer
    extends MapReduceBase
    with Reducer[Text, IntWritable, Text, IntWritable] {
    val logger: Logger = Logger.getLogger(getClass.getName)

  /***
   * Reduce Function Implements Reduce task
   * @param key Text
   * @param values util.Iterator[IntWritable]
   * @param output OutputCollector[Text, IntWritable]
   * @param reporter Reporter
   * @throws IOException
   */
  @throws[IOException]
  def reduce(
      key: Text,
      values: util.Iterator[IntWritable],
      output: OutputCollector[Text, IntWritable],
      reporter: Reporter
  ) {
    logger.info("Inside Reduce")
    try {
    import scala.collection.JavaConversions._
    val sum = values.toList.reduce((valueOne, valueTwo) =>
      new IntWritable(valueOne.get() + valueTwo.get())
    )
    logger.info(key+"   ->  "+ sum )
    output.collect(key, new IntWritable(sum.get()))
  }
    catch {
      case ex: Exception =>
        println(ex.getMessage)
    }
  }
}
