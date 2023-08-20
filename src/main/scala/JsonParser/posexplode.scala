package JsonParser

import JsonParser.FriendRecommendation.spark
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
object posexplode {

  def main(args: Array[String]): Unit = {

    val df: Dataset[String] = spark.read.textFile(
      "D:\\test\\friend_connection\\userInteractionData.txt"
    )


    import spark.implicits._
    val relationships = df
      .selectExpr(
        "split(value, ' ')[0] as user",

        "posexplode(split(value, ' ')) as (pos, friend)",
        "split(value, ' ')[size(split(value, ' ')) - 1] as last_active_time"
      )
      .filter($"pos" > 0 && $"pos" < size(split($"value", " ")) - 1)
//      .drop("pos")

    relationships.show()
  }

}
