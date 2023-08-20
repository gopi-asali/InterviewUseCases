import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object SchemaEvolutionExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SchemaEvolution")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._

    // Initial schema
    val initialSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("age", IntegerType, nullable = true)  // Added a new column
    ))

    // Create DataFrame with initial schema
    val initialData = Seq((1, "Alice", 25), (2, "Bob", 30))
    val initialDF = initialData.toDF("id", "name", "age")

    // Write initial data to Parquet format
    initialDF.write.mode("overwrite").parquet("D:/test/schema")
    initialDF.write.mode("overwrite").parquet("D:/test/schema2")

    spark.read
//      .schema(updatedSchemaBackward)
      //      .option("mergeSchema", "true")  // Enable schema evolution
      .parquet("D:/test/schema").show()
    // Updated schema (backwards compatible)
    val updatedSchemaBackward = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false)
    ))

    // Load the initial data with updated schema (backwards compatible)
    val updatedDFBackward = spark.read
      .schema(updatedSchemaBackward)
//      .option("mergeSchema", "true")  // Enable schema evolution
      .parquet("D:/test/schema2")

    // Show the updated DataFrame (backwards compatible)
    println("Updated DataFrame (Backwards Compatible):")
    updatedDFBackward.show()
    updatedDFBackward.write.mode("overwrite").parquet("D:/test/schema")

    spark.read
//      .schema(updatedSchemaBackward)
      //      .option("mergeSchema", "true")  // Enable schema evolution
      .parquet("D:/test/schema").show()

    // Updated schema (forward compatible)
    val updatedSchemaForward = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("age", IntegerType, nullable = true),  // Added a new column
      StructField("city", StringType, nullable = true)   // Added another new column
    ))

    // Load the initial data with updated schema (forward compatible)
    val updatedDFForward = spark.read
      .schema(updatedSchemaForward)
//      .option("mergeSchema", "true")  // Enable schema evolution
      .parquet("D:/test/schema2")
    updatedDFForward.write.mode("overwrite").parquet("D:/test/schema")
    // Show the updated DataFrame (forward compatible)
    println("Updated DataFrame (Forward Compatible):")
    updatedDFForward.show()

    spark.read
//      .schema(updatedSchemaForward)
      //      .option("mergeSchema", "true")  // Enable schema evolution
      .parquet("D:/test/schema").show()

    spark.stop()
  }
}
