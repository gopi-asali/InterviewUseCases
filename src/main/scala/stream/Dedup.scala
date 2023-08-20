package stream

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, first}

import java.util.concurrent.Executors
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps

object Dedup {

  def main(args: Array[String]): Unit = {
val      spark = SparkSession.builder.appName("DedupExample")
  .master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
      val data = Seq(
    ("A", 1, "X"),
    ("B", 2, "Y"),
    ("A", 1, "X"),
    ("C", 3, "Z"),
    ("B", 2, "Y"),
    ("D", 4, "W"),
    )
    import scala.concurrent.duration._
    import scala.concurrent.ExecutionContext.Implicits.global
//    implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2))
    val x = Future{
      println("Hi")
      "hello"
    }

    println("nope")
    val y =Await.result(x, 10.seconds)

    println(y)
    import spark.implicits._
    val df = spark.sparkContext.parallelize(data).toDF("col1","col2","col3")

    val deduplicated_df = df.groupBy("col1", "col2").agg(
      first(col("col3"))
    )

//      deduplicated_df.show()

  }

}
