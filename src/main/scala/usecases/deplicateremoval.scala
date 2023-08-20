package usecases

import org.apache.spark.sql.SparkSession

object RemoveDuplicates {
  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().master("local").appName("dup").getOrCreate()

    val in =  Seq(userData("gopi", 27, "salem"),
      userData("gopi", 27, "salem"),
      userData("gopi", 27, "salem"))
    val rdd = session.sparkContext.parallelize(in
    )

    val dupRemovedDataset = rdd.map(data => ((data.name, data.age), data)).reduceByKey((val1, _) => val1).map(_._2).collect()

    dupRemovedDataset.foreach(value => println(value.toString))

  }
}


case class userData(name: String, age: Int, location: String)