import com.WordCount.WordCount
import org.scalatest.{FlatSpec, Matchers, _}

import scala.io.Source


class WordCountIntegrationSpec
    extends FlatSpec
    with Matchers
    with BeforeAndAfterEach {

  "com/WordCount" should "write out word counts to output folder" in {
    WordCount.main(Array())

    val output = Source.fromFile("output/part-00000").mkString
    output should equal("""|Kafka	1
                          |The	1
                          |a	3
                          |amongst	1
                          |as	2
                          |belonging	1
                          |by	2
                          |consumed	1
                          |consumer	2
                          |consumers	2
                          |divides	1
                          |each	1
                          |establishing	1
                          |fairly	1
                          |from	1
                          |group	4
                          |id.	1
                          |in	1
                          |is	1
                          |only	1
                          |partition	1
                          |partitions	1
                          |possible	1
                          |same	1
                          |share	1
                          |single	1
                          |that	1
                          |the	3
                          |themselves	1
                          |then	1
                          |to	1
                          |topic	1
         |""".stripMargin)
  }
}
