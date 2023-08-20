/*
package stream

import org.apache.spark.sql.SparkSession

object primarykey {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("deltawrite")
      .getOrCreate()

    import org.apache.spark.sql.functions._
import spark.implicits._
    // Create the source DataFrame
    val df_source = spark.sparkContext.parallelize(Seq(("1", "John Doe", 30), ("2", "Jane Doe", 25))).toDF("id", "name", "age")

    // Create the target Delta Lake table
    val df_target = spark.read.format("delta").load("/path/to/target/table")

    // Define the merge condition
    val merge_condition = col("df_source.id") === col("df_target.id")
    import io.delta.implicits._
    // Define the update actions
    val update_actions = Seq(
      when(merge_condition, $"df_target.name" := $"df_source.name"),
      when(merge_condition, $"df_target.age" := $"df_source.age")
    )

    // Merge the DataFrames
    val df_merged = df_source.merge(df_target, merge_condition, update_actions)

    // Write the merged DataFrame to the Delta Lake table
    df_merged.write.format("delta").save("/path/to/target/table")


  }

}
*/
