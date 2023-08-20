package usecases

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._

object Test {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("test")
      .getOrCreate()

    /*val sourceDf = spark.table("source.hivetable")

    val formatdf = sourceDf.withColumn(
      "year_month",
      date_format(col("update_ts"), "yyyyMM")
    )

    formatdf.write
      .partitionBy("year_month")
      .mode("append")
      .format("parquet")
      .saveAsTable("table_a")

    val df2 = spark.table("source.second_hivetable")

    val filteredDf =
      df2.filter("update_ts  >= date_subtract(current_timstamp(), -6,M) ")

    val updatedDf = filteredDf.withColumn(
      "date_value",
      date_format(col("update_ts"), "yyyyMMdd")
    )

    updatedDf.write
      .partitionBy("year_month")
      .mode("append")
      .format("parquet")
      .saveAsTable("table_b")*/

    val tableA = spark.table("source.hivetable").repartition(col("id"))
    val tableB = spark.table("source.hivetable").repartition(col("id"))

    val joinedDf = tableA.join(tableB, Seq("id"), "left").repartition(200)

    val filterdDf =
      joinedDf.filter("update_ts  >= date_subtract(current_timstamp(), -6,M) ")

    filterdDf.write.partitionBy("year_month")
      .mode("append")
      .format("parquet")
      .saveAsTable("table_b")

  }

}
