package JsonParser

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

object Main {
def main(args: Array[String]): Unit = {
  val spark: SparkSession = SparkSession.builder.appName("test").master("local[2]").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  val fs: FileSystem = FileSystem.get(new Configuration())
  val parentNode: String = ""
  val LoadType: String = "full"
  val src_path: String = "C:\\Users\\HP EliteBook\\Downloads\\inp3.json"
  val out_path: String = "C:\\Users\\S441633\\OneDrive - Emirates Group\\ENIT\\Code\\json_parsing\\cyberflat\\"
  var src_df: DataFrame = spark.read.json(src_path)
  var par_col_name: String = ""
  var curr_level_cols = src_df.columns.toSeq
  println("main level cols - " + curr_level_cols)
  var structcol_name = ""
  println("Source Schema is ")
  src_df.printSchema()
  var upd_df = src_df
  var name_schema_map = scala.collection.mutable.Map[String, String] ()

  def def_name_schema_map (curr_level_cols_fn: Seq[String], src_df:DataFrame): scala.collection.mutable.Map[String,String] = {
    var name_schema_map_fn = scala.collection.mutable.Map[String, String] ()
    for (names <- curr_level_cols_fn){
      name_schema_map_fn += names -> src_df.schema (names).dataType.typeName
      }
    name_schema_map_fn
    }

  name_schema_map = def_name_schema_map(curr_level_cols, src_df)

 while (name_schema_map.exists(_._2 == "struct")) {
   println(name_schema_map)
  for (names <- name_schema_map){
//    println("Key : "+names._1+", Value :"+names._2)
    println(names)
    if(names._2 == "struct"){
      println(names._2)
      var pass_name = names._1
      var sub_strcut_col_names = src_df.select((s"$pass_name.*")).columns.toSeq
      for (sub_names <- sub_strcut_col_names) {
        println(sub_names)
        src_df =  src_df.withColumn(pass_name+"_"+sub_names,col(pass_name+"."+sub_names ))
       }
      println(src_df.columns.toSeq)
      src_df =  src_df.drop(names._1)
      }
    println("6")
  }
   curr_level_cols =  src_df.columns.toSeq
   name_schema_map =  def_name_schema_map(curr_level_cols, src_df)
   println(curr_level_cols)
 }


  src_df.show(false)
 /* src_df.write
      .format("parquet")
      .mode("overwrite")
      .save("C:\\Users\\S441633\\OneDrive - Emirates Group\\ENIT\\Code\\json_parsing\\cyberflat\\")*/
  println("Final Data Schema")
  src_df.printSchema()
}
}

