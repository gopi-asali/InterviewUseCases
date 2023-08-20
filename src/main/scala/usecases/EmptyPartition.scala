package usecases

import org.apache.spark.sql.SparkSession

object EmptyPartition {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Empty")
      .getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    val df = spark.read.csv(
      "D:\\test\\Uploads_Annual-enterprise-survey_Annual-enterprise-survey-2021-financial-year-provisional_Download-data_annual-enterprise-survey-2021-financial-year-provisional-size-bands-csv.csv"
    )

    println(df.rdd.getNumPartitions)

    val updateddf = df.where("1=2").repartition(10)

    println(updateddf.rdd.getNumPartitions)
    updateddf.write.mode("overwrite").format("csv").option("header", "true").save("D:\\test\\empty\\")
    println(updateddf.count())
  }
}
