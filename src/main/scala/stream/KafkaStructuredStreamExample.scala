package stream

import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaStructuredStreamExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("KafkaStructuredStreamExample")
      .getOrCreate()

    spark.sparkContext.setLogLevel("OFF")

    val kafkaBrokers = "localhost:9092" // Replace with your Kafka broker list
    val kafkaTopic = "dcb" // Replace with your Kafka topic

    val kafkaStream: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", kafkaTopic)
//      .option("kafka.auto.offset.reset", "earliest")
      .option("startingOffsets", "earliest")
      .option("enable.auto.commit", "true")
      .load()

    val valueStream = kafkaStream.selectExpr("CAST(value AS STRING)")

    // Define your processing logic here
    val processedStream = valueStream
      .writeStream
      .outputMode("append")
      .option("checkpointLocation", "D:\\test\\kafka/checkpoint")
      .format("console")
      .start()
    val writestream = valueStream
      .writeStream
      .outputMode("append")
      .option("checkpointLocation", "D:\\test\\kafka/checkpoint")
      .format("console")
      .start()


    processedStream.awaitTermination()
  }
}
