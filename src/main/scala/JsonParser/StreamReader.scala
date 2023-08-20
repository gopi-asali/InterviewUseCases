package JsonParser
import org.apache.spark.sql._
object StreamReader {

  val spark= SparkSession.builder()
    .master("local[*]")
//    .enableHiveSupport()
    .appName("test")
    .getOrCreate()


  def main(args: Array[String]): Unit = {


  val df = spark.readStream
    .text("D:\\Documents\\Documents\\In")



   val query= df.writeStream.format("text")
     .option("checkpointLocation", "D:\\App\\checkpoint1")
     .outputMode("append").start("D:\\Documents\\Documents\\out")


    query.awaitTermination()


  }

}
