package JsonParser

import org.apache.spark.sql.SparkSession

object JsonMultiSchema {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession =
      SparkSession.builder().master("local[*]").appName("json").getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    val schemadf =
      spark.read.option("multiLine", "true").json("D:\\test\\shema.json")
    schemadf.show()
    spark.read
      .option("multiLine", "true")
      .schema(schemadf.schema)
      .json("D:\\test\\sample.json")
      .show()
  }
}
