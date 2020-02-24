import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.immutable.List
import scala.collection.JavaConverters._
import java.io.{BufferedWriter, FileWriter}
import au.com.bytecode.opencsv.CSVWriter
import scala.util.{Failure, Try}

package word_count {

  import java.nio.charset.Charset

  import org.apache.spark.rdd.RDD

  object HelloWorld {

    val header: List[String] =
      List("World", "Count")

    def writeCsvFile(
                      fileName: String,
                      header: List[String],
                      rows: List[List[String]]
                    ): Try[Unit] =
      Try(new CSVWriter(new BufferedWriter(new FileWriter(fileName))))
        .flatMap((csvWriter: CSVWriter) =>
        Try{
          csvWriter.writeAll(
            (header +: rows).map(_.toArray).asJava
          )
          csvWriter.close()
        } match {
          case f @ Failure(_) =>
            // Always return the original failure.  In production code we might
            // define a new exception which wraps both exceptions in the case
            // they both fail, but that is omitted here.
            Try(csvWriter.close()).recoverWith{
              case _ => f
            }
          case success =>
            success
        }
      )

    def main(args: Array[String]): Unit = {

      val toRemove = ",.!\"'?():;`".toSet

      val conf = new SparkConf().
        setMaster("local").
        setAppName("LearnScalaSpark")
      val sc = new SparkContext(conf)

      val rpsteam=sc.textFile("Text_Alice/alice_in_wonderland.txt");
      val rpscricket=rpsteam
        .flatMap(lines=>lines.split(" "))
        .map(string=>string.toUpperCase.filterNot(toRemove))
        .filter(word=>(!word.equals("")))
        .map(word=>(word,1))
        .reduceByKey(_+_);

      writeCsvFile("Text_Alice/alice_in_wonderland_result.csv",
        header,
        (rpscricket.sortBy(_._2)
          .collect()
          .reverse
          .take(100)
          .map(tuple=>List(tuple._1, tuple._2.toString))
        ).toList)
    }

  }

}
