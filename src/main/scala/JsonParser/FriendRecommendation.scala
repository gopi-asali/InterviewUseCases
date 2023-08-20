package JsonParser

import org.apache.spark.sql.functions.{col, count, explode, expr}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.sql.Timestamp

case class friend_inter(
    friends: Array[String],
    last_interaction_time: Timestamp
)
case class user(name: String, friend_inter: friend_inter)
object FriendRecommendation {

  val spark: SparkSession =
    SparkSession.builder().master("local[*]").appName("recommed").getOrCreate()

  def main(args: Array[String]): Unit = {
    spark.sparkContext.setLogLevel("OFF")
    println("Hello")

    val schema = StructType(Seq(StructField("name", StringType, false)))

    import spark.implicits._
    val df: Dataset[String] = spark.read.textFile(
      "D:\\test\\friend_connection\\userInteractionData.txt"
    )

    val usersList: Dataset[user] = df.flatMap(value => {

      val data = value.trim.split("\\s")

      val top: String = data.head
      val last: String = data.last

      val friends = data.filterNot(Seq(data.head, data.last).contains)

      import java.sql.Timestamp
      import java.text.SimpleDateFormat

      val inputTimestampStr = last // Example: "2023-08-06 12:00:00"
      val format = new SimpleDateFormat("yyyyMMddHH")
      val parsedDate = format.parse(inputTimestampStr)
      val timestamp = new Timestamp(parsedDate.getTime)
//      println(timestamp)
      user(top, friend_inter(friends, timestamp)) +: friends.map(friend => {
        user(friend, friend_inter(Array(top), timestamp))
      })
    })

    val frields = usersList.selectExpr("name", "friend_inter.*")

    val targetDf: DataFrame =
      frields.withColumn("friends", explode(col("friends")))

    val sourcedf = targetDf.columns
      .map(column => (column, s"${column}_source"))
      .foldLeft(targetDf)((df, column) => {
        df.withColumnRenamed(column._1, column._2)
      })
    val rightdf = targetDf.columns
      .map(column => (column, s"${column}_target"))
      .foldLeft(targetDf)((df, column) => {
        df.withColumnRenamed(column._1, column._2)
      })

    val friendSuggestion = sourcedf
      .as("source")
      .join(
        rightdf.as("target"),
        expr(
          "source.friends_source = target.name_target and source.name_source != target.name_target " +
            " and source.name_source != target.friends_target"
        )
      )

    friendSuggestion
      .groupBy("name_source","friends_source", "friends_target")
      .agg(count("friends_target").as("connect_count")).where("connect_count>2")
      .show()

  }
}
