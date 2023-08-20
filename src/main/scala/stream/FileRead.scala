package stream

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object FileRead {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
//      .enableHiveSupport()
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .appName("deltawrite")
      .getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._
    import io.delta.implicits._
    val df = spark.sparkContext
      .parallelize(
        Seq(
          ("gopi", "salem", "2022-01-01 09:01"),
          ("gopi", "salem", "2022-01-01 09:01")
        )
      )
      .toDF("name", "location", "arrive_time")

//    df.show()
    df.where("1=2")
      .write
      .mode("overwrite")
      .format("delta")
      .option("path", "D:\\test\\detla\\merge")
      .saveAsTable("delta_temp")

    spark.sql("select * from delta_temp").show()

    df.write
      .mode("overwrite")
      .format("delta")
      .option("path", "D:\\test\\detla\\merge")
      .saveAsTable("delta_temp")

    val df2 = spark.sparkContext
      .parallelize(
        Seq(
          ("gopi", "singpore", "2022-01-02 10:01")
      , ("gopi","usa","2022-01-02 11:01")
        )
      )
      .toDF("name", "location", "arrive_time")

    val sourcedf = DeltaTable.forPath("D:\\test\\detla\\merge")

    sourcedf.toDF.show()
    sourcedf
      .alias("target")
      .merge(df2.alias("source"), "source.name=target.name")
      .whenMatched("target.arrive_time < source.arrive_time ")
      .update(Map("target.location" -> col("target.location")))
      .whenNotMatched()
      .insertAll()
      .execute()

    val sourcedf1 = DeltaTable.forPath("D:\\test\\detla\\merge")

    sourcedf1.toDF.show()

  }

}
