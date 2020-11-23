package com.WordCount

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred._

object WordCount {
  @throws[Exception]
  def main(args: Array[String]) {
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("WordCountScala")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[WordCountMapper])
    conf.setCombinerClass(classOf[WordCountReducer])
    conf.setReducerClass(classOf[WordCountReducer])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path("input"))
    FileOutputFormat.setOutputPath(conf, new Path("output"))
    JobClient.runJob(conf)
  }
}
