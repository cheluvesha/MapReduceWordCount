/***
 * Dependency used Scala-test and Mrunit
 */

import java.util
import com.WordCount.{WordCountMapper, WordCountReducer}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mrunit.{MapDriver, MapReduceDriver, ReduceDriver}
import org.scalatest.{BeforeAndAfter, FunSuite}

/***
 * WordCountSpec Which Verifies Mapper And Reducer Class
 */
class WordCountSpec extends FunSuite with BeforeAndAfter {
  var mapDriver: MapDriver[LongWritable, Text, Text, IntWritable] = _
  var reduceDriver: ReduceDriver[Text, IntWritable, Text, IntWritable] = _
  var mapReduceDriver: MapReduceDriver[
    LongWritable,
    Text,
    Text,
    IntWritable,
    Text,
    IntWritable
  ] = _

  def setUp(): Unit = {
    val mapper = new WordCountMapper
    mapDriver = new MapDriver[LongWritable, Text, Text, IntWritable]()
    mapDriver.setMapper(mapper)

    val reducer = new WordCountReducer
    reduceDriver = new ReduceDriver[Text, IntWritable, Text, IntWritable]()
    reduceDriver.setReducer(reducer)

    mapReduceDriver = new MapReduceDriver[
      LongWritable,
      Text,
      Text,
      IntWritable,
      Text,
      IntWritable
    ]()
    mapReduceDriver.setMapper(mapper)
    mapReduceDriver.setReducer(reducer)
  }

  before {
    setUp()
  }

  test("givenDataToMapperItHasToConvertItIntoKeyValuePair") {
    mapDriver.withInput(new LongWritable(1), new Text("orange orange apple"))
    mapDriver.withOutput(new Text("orange"), new IntWritable(1))
    mapDriver.withOutput(new Text("orange"), new IntWritable(1))
    mapDriver.withOutput(new Text("apple"), new IntWritable(1))
    mapDriver.runTest()
  }

  test("givenWhenDataToReducerItMustProcessItShouldReturnOutput") {
    val values: util.ArrayList[IntWritable] = new util.ArrayList[IntWritable]()
    values.add(new IntWritable(1))
    values.add(new IntWritable(1))
    reduceDriver.withInput(new Text("orange"), values)
    reduceDriver.withOutput(new Text("orange"), new IntWritable(2))
    reduceDriver.runTest()
  }
  test("givenKeyValueDataMustValidateMapReduceJobCompletely") {
    mapReduceDriver.addInput(
      new LongWritable(1),
      new Text("orange orange apple")
    )
    mapReduceDriver.addOutput(new Text("apple"), new IntWritable(1))
    mapReduceDriver.addOutput(new Text("orange"), new IntWritable(2))
    mapReduceDriver.runTest()
  }

}
