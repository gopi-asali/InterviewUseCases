package usecases

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object CelluarTest {

  def main(args: Array[String]): Unit = {



    val spark =
      SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    val csvDf: DataFrame =  spark.read
      .option("header", true)
      .csv("C:\\Users\\gopasali\\Documents\\Call_History.csv")



    println(csvDf.rdd.partitions.length)
    val outdf = 1.to(10).foldLeft(csvDf)((df,_) => df.union(df)).cache()

    println(outdf.count())

    outdf.repartition(100).write.mode("Overwrite").csv("C:\\Users\\gopasali\\Documents\\testPartition\\")

    val seondDf : DataFrame =  spark.read
      .option("header", true)
      .csv("C:\\Users\\gopasali\\Documents\\testPartition\\")

    println(seondDf.rdd.partitions.length)
    println(seondDf.count())



    import org.apache.spark.sql.functions._

    val first_string = "OENDS932"
    val second_string = "2ND3O9SE"

    println(encode_strings(first_string, second_string))

    def encode_strings(first_string: String,
                       second_string: String): (String, String) = {
      val position: String => String => String = first_string =>
        second_string =>
          first_string.map(char => second_string.indexOf(char)).mkString("")
      (
        position(first_string)(second_string),
        position(second_string)(first_string)
      )
    }


    seondDf.filter(
        col("availability_flag") === "Y" && !col("discount_shown_flag ") === "Y"
      )
      .groupBy(to_date(col("request_time")), col("product_id"))
      .agg(count(col("product_id").as("count")))
      .filter(col("count") > 0).show()

    System.exit(0)

    val df = spark.read
      .option("header", true)
      .csv("C:\\Users\\gopasali\\Documents\\Call_History.csv")
    df.show(false)

    df.createOrReplaceTempView("phonebook")
    spark.sql(
      "SELECT customer_id, AVG(second(CAST(call_start_day AS TIMESTAMP) - CAST(call_end_day AS TIMESTAMP))) average FROM phonebook WHERE call_start_day >= add_months(now(), -13)  AND call_start_day <  add_months(now(),-1)GROUP BY customer_id ORDER BY 2 DESC"
    )



    val s="\n"

    val rows: Array[String] = s.split("\n")
    val columnRow: Array[StructField] = rows.head.split(",").map(name => StructField(name,StringType))

    val rdd = spark.sparkContext.parallelize(rows.tail).map((values: String) => {
      Row.fromSeq(values.split(","))
    })


   val newDf: DataFrame =  spark.createDataFrame(rdd, StructType(columnRow))





    newDf.columns.foldLeft(newDf)((df,column) => {
      df.filter(col(column)=!="NULL")
    })








  }

}
