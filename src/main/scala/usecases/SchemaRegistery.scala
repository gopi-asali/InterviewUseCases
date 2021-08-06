package usecases

object SchemaRegistery extends App {/*

  //  val schemaRegistryAddress = kafkaCluster.schemaRegistryAddress
  //  val schemaRegistryClient = kafkaCluster.schemaRegistryClient


  lazy val schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryAddress, 1000)
  val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
  val schemaRegistryAddress = "http://10.73.114.118:8081"
  val input2 = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", schemaRegistryAddress)
    .option("subscribe", "input2")
    .load()
import spark.implicits._

  // COMMAND ----------
  val df1 = Seq(("Welcome", "toBigdata")).toDF("a", "b")
    .map(row => myCaseClass(Some(row.getAs("a")), Some(row.getAs("b"))))

  // COMMAND ----------


  import spark.implicits._
  val cols = df1.columns
  df1.show

  case class myCaseClass(first: Option[String] = None, second: Option[String] = None)

  // COMMAND ----------

  //spark.range(5)
  //.select(struct('id.cast("string").as('first), 'id.cast("string").as('second)).as('struct))
  df1.select(struct(cols.map(column): _*).as('struct))
//    .select(from_avro('struct, "defaultNullCheck38").as('value))
    .show
  // .write
  //.format("kafka")
  //.option("kafka.bootstrap.servers", "10.73.114.82:9092, 10.73.114.77:9092, 10.73.114.78:9092")
  //.option("topic", "chandra2")
  //.save()

  // COMMAND ----------

  import scala.sys.process._

  Seq("curl", "-X", "GET", s"$schemaRegistryAddress/subjects/").!

  // COMMAND ----------

*/
}
