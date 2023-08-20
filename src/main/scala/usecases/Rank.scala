package usecases

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, rank, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Rank {


  def main(args: Array[String]): Unit = {
    //    C:\Users\HP EliteBook\Desktop\stream\input.txt
    val spark = SparkSession.builder().master("local").appName("rank").getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._
    val simpleData = Seq(("James","Sales","NY",90000,34,10000),
      ("Michael","Sales","NY",86000,56,20000),
      ("Robert","Sales","CA",81000,30,23000),
      ("Maria","Finance","CA",90000,24,23000),
      ("Raman","Finance","CA",99000,40,24000),
      ("Scott","Finance","NY",83000,36,19000),
      ("Jen","Finance","NY",79000,53,15000),
      ("Jeff","Marketing","CA",80000,25,18000),
      ("Kumar","Marketing","NY",91000,50,21000)
    )
    val df: DataFrame = simpleData.toDF("employee_name","department","state","salary","age","bonus")
    spark.conf.set("spark.sql.adaptive.enabled",true)
    df.show()
    val df1=df.groupBy("department").pivot("state").sum("salary")
    println(df1.rdd.getNumPartitions)
    df1.show()





//    val df = spark.read.option("header","true").csv("C:\\Users\\HP EliteBook\\Desktop\\stream\\input1.txt")

    /*df.show()

    df.createOrReplaceTempView("data_table")

    spark.sql("select post_id, sum(case when event='like' then 1 else 0 end)-sum(case when event='dislike' then 1 else 0 end) net_like from data_table group by post_id").show()

    spark.sql("select * from " +
      "(select *,rank() over( order by net_like desc) as rank from " +
      "(select post_id, sum(case when event='like' then 1 else 0 end)-sum(case when event='dislike' then 1 else 0 end) net_like " +
      "from data_table group by post_id )x )y where rank=1").show()*/

//    val df = spark.read.format("csv").load("")


    def printTopValue(df: DataFrame,top:Int)={

      df.withColumn("row", rank().over(Window.partitionBy("plant_name").orderBy(col("Finished_Product").desc)))
        .where(s"row=$top")
        .show()

    }

//    printTopValue(df,1)


  }

}
