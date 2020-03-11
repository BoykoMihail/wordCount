
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.immutable.List
import scala.collection.JavaConverters._
import java.io.{BufferedWriter, FileWriter}
import au.com.bytecode.opencsv.CSVWriter
import scala.util.{Failure, Try}

object HelloSpark {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("LearnScalaSpark")
      .set("spark.driver.bindAddress", "127.0.0.1")
    val sc = new SparkContext(conf)

    val rpsteam=sc.textFile("Text_Alice/alice_in_wonderland.txt");
    val rpscricket=rpsteam
      .flatMap(lines=>lines
        .split("\\p{Punct}|\\p{Blank}"))
      .map(string=>string.toUpperCase)
      .filter(word=>(!word.equals("")))
      .map(word=>(word,1))
      .reduceByKey(_+_);

    rpscricket.sortBy(_._2, false)
      .map(tuple=>Array(tuple._1, tuple._2).mkString(","))
      .coalesce(1,false)
      .saveAsTextFile("Text_Alice/alice_in_wonderland_result2.csv")
  }

}
