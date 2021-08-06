package usecases

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.types._

object SparkRegexCheck {
  def main1(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("").master("").getOrCreate()

    val sqlContext = session.sqlContext

    val df_avro = sqlContext.read
      .format("com.databricks.spark.avro")
      .load("/dev/internal/bosc_pi/clean/RateManagementCDP/*.avro")

    val df_regex = df_avro.columns.foldLeft(df_avro) { (df_regex, colName) =>
      df_regex.withColumn(
        colName,
        regexp_replace(col(colName), "Bad.*", "null")
      )
    }

    val df_cols = df_regex.select(
      col("Id").cast(LongType),
      col("Rate_Man_ID").cast(StringType),
      col("Event_Start_Time").cast(StringType),
      col("Event_End_Time").cast(StringType),
      col("Event_Duration").cast(IntegerType),
      col("TimeStamp").cast(StringType),
      col("Actual_Volume_Loc").cast(DoubleType),
      col("Actual_Volume_Imp").cast(DoubleType),
      col("Actual_Volume_Met").cast(DoubleType),
      col("Location").cast(StringType),
      col("Area").cast(StringType),
      col("Area_Type").cast(StringType),
      col("Best_Dem_Avg_Loc").cast(DoubleType),
      col("Best_Dem_Avg_Met").cast(DoubleType),
      col("Best_Dem_Avg_Imp").cast(DoubleType),
      col("Best_Dem_Q3_Cap_Loc").cast(DoubleType),
      col("Best_Dem_Q3_Cap_Met").cast(DoubleType),
      col("Best_Dem_Q3_Cap_Imp").cast(DoubleType),
      col("Best_Dem_Stab_Factor").cast(DoubleType),
      col("CD_Id_Preserved").alias("IP_NonIP").cast(StringType),
      col("CD_Process_Setup").alias("Process_Setup").cast(StringType),
      col("CD_Product").alias("Product").cast(StringType),
      col("CD_Prod_Family").alias("Product_Family").cast(StringType),
      col("CD_Prod_Origin").alias("Product_Origin").cast(StringType),
      col("CD_Volume_Loc").cast(DoubleType),
      col("CD_Volume_Imp").cast(DoubleType),
      col("CD_Volume_Met").cast(DoubleType),
      col("Conv_Factor_Imp").cast(DoubleType),
      col("Conv_Factor_Met").cast(DoubleType),
      col("End_Time_UTC").cast(StringType),
      col("End_Time_Plant_TimeZone").cast(StringType),
      col("Imperial_Volume_UoM").cast(StringType),
      col("Last_Updated_Date").cast(StringType),
      col("Local_Volume_UoM").cast(StringType),
      col("Metric_Volume_UoM").cast(StringType),
      col("ODV_Average_Loc").cast(DecimalType(15, 6)),
      col("ODV_Average_Imp").cast(DoubleType),
      col("ODV_Average_Met").cast(DoubleType),
      col("ODV_Set_Point_Loc").cast(DoubleType),
      col("ODV_Set_Point_Imp").cast(DoubleType),
      col("ODV_Set_Point_Met").cast(DoubleType),
      col("Planned_Vol_Loc").cast(DoubleType),
      col("Planned_Vol_Imp").cast(DoubleType),
      col("Planned_Vol_Met").cast(DoubleType),
      col("Region").cast(StringType),
      col("StartTime").cast(StringType),
      col("FM").cast(IntegerType),
      col("FQ").cast(StringType),
      col("FY").cast(StringType),
      col("StartTime_UTC_YYYY_MM_DDThh_mm")
        .alias("StartTime_UTC")
        .cast(StringType),
      col("StartTime_YYYY_MM_DDThh_mm").alias("StartTime_Loc").cast(StringType),
      col("Stock_Adj_Loc").cast(DoubleType),
      col("Stock_Adj_Imp").cast(DoubleType),
      col("Stock_Adj_Met").cast(DoubleType),
      col("To_Process_Moisture").cast(DoubleType),
      col("PIIntTSTicks").cast(LongType),
      col("PIIntShapeID").cast(LongType)
    )

    val final_df = df_cols.columns.foldLeft(df_cols) { (df_nonNull, colName) =>
      df_nonNull.schema(colName).dataType match {
        case LongType    => df_nonNull.na.fill(0, Seq(colName))
        case StringType  => df_nonNull.na.fill(null, Seq(colName))
        case DoubleType  => df_nonNull.na.fill(0.0, Seq(colName))
        case IntegerType => df_nonNull.na.fill(0, Seq(colName))
        case _           => df_nonNull
      }
    }

    final_df.write
      .format("com.databricks.spark.avro")
      .save("/dev/internal/bosc_pi/clean/RateManagementCDP_clean")

  }

  def main(args: Array[String]): Unit = {
    val session =
      SparkSession.builder().appName("dsd").master("local[*]").getOrCreate()

    import org.apache.spark.sql.functions._
    import session.implicits._
    session.sparkContext.setLogLevel("OFF")

    val df = session.sparkContext
      .parallelize(
        Seq(

          ("speed_v1", 123),
          ("", 1233)
        )
      )
      .toDF("name", "id")
    println(df.rdd.partitions.length)

    df.where(col("name").isNotNull && col("name") =!= lit("")).show()
//    val df2 = df.withColumn("row", row_number().over(Window.partitionBy("name","id").orderBy("id")))
//
//    println(df2.rdd.partitions.length)
//
//    df2.show(false)

  }

  def main2(args: Array[String]): Unit = {

    val session =
      SparkSession.builder().appName("test").master("local").getOrCreate()

    Logger.getRootLogger().setLevel(Level.OFF)
    import session.implicits._

    val df = session.sparkContext
      .parallelize(List(("Gopi", null), ("nandi", "1.765765765765")))
      .toDF("Name", "Address")
      .withColumn("Address", col("Address").cast("decimal(10,5)"))

    val in = 0.00000
    df.printSchema()
    df.na.fill(in, Array("Address")).show()

  }
}
