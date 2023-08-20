import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
object MutualFriendsSuggestions {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MutualFriendsSuggestions")
      .getOrCreate()

    import spark.implicits._

    // Load the data and create a DataFrame
    val data = spark.read.text("path_to_your_data_file")

    // Split the data and create a DataFrame with columns: user, friend, last_active_time
    val relationships = data
      .selectExpr(
        "split(value, ' ')[0] as user",
        "posexplode(split(value, ' ')) as (pos, friend)",
        "split(value, ' ')[size(split(value, ' ')) - 1] as last_active_time"
      )
      .filter($"pos" > 0 && $"pos" < (size(split(col("value"), " ")) - 1))
      .drop("pos")

    // Group by friend and collect a list of users
    val groupedUsers = relationships
      .groupBy("friend", "last_active_time")
      .agg(collect_list("user").as("users"))

    // Self-join to find mutual friends
    val mutualFriends = groupedUsers.alias("a")
      .join(groupedUsers.alias("b"), Seq("last_active_time"))
      .where($"a.friend" < $"b.friend") // To avoid duplicate pairs

    // Calculate mutual friends and filter non-empty cases
    val suggestedMutualFriends = mutualFriends
      .withColumn("mutual_friends", array_intersect($"a.users", $"b.users"))
      .filter(size($"mutual_friends") > 0)

    // Display the results
    suggestedMutualFriends
      .select("a.friend", "b.friend", "mutual_friends")
      .orderBy("a.friend", "b.friend")
      .show(false)

    spark.stop()
  }
}
