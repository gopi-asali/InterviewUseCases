package JsonParser

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ShuffleCal {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("largedata")
      .master("local[*]")
//      .config("spark.sql.adaptive.enabled", "true")
//      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()


    val active_cores = spark.sparkContext.defaultParallelism
    print("Number of active CPU cores:", active_cores)



    spark.sparkContext.setLogLevel("OFF")
    val sourceDf = spark.read
      .option("header", "true")
      .csv(
        "D:\\test\\Uploads_Annual-enterprise-survey_Annual-enterprise-survey-2021-financial-year-provisional_Download-data_annual-enterprise-survey-2021-financial-year-provisional-csv.csv"
      )
      .cache()

    println("Initial partitions", sourceDf.rdd.getNumPartitions)
    println("The records count", sourceDf.count())

    val unionDf = (0 to 10).foldLeft(sourceDf)((sourceDf, in) => {
      sourceDf
        .union(sourceDf)

    })
    println("unionDf partitions", unionDf.rdd.getNumPartitions)
    val aggdf = unionDf.groupBy(unionDf.columns(0)).agg(sum("Units"))
    println("Target partitions", aggdf.rdd.getNumPartitions)
    aggdf.count()

/*    println("After union the partition", unionDf.rdd.getNumPartitions)

    unionDf.write.mode("overwrite").parquet("D:\\test\\spark_result/shuffle")

    val df1 = spark.read.parquet("D:\\test\\spark_result/shuffle")

    println("Partition reading target", df1.rdd.getNumPartitions)

    println("Target count", df1.count())*/

//    df1.selectExpr("SUM(CAST(sizeInBytes AS bigint)) AS totalSize").show()

  }

}
