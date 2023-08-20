import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, explode, explode_outer, lit, udf}
import org.apache.spark.sql.types._

import java.time.Instant
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

//  Covered
object JsonLoader {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder.appName("test").master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val fs: FileSystem = FileSystem.get(new Configuration())

    //    val filename = "C:\\Users\\S441633\\OneDrive - Emirates Group\\ENIT\\Code\\IssueLinksParse\\INPUT.json"
    //    val ordersDf = spark.read.option("multiline","true").json(filename)
    //  ordersDf.printSchema()
    // Covered
    def flattenJsonData(path: String, parentNode: String = null): DataFrame = {
      val df: DataFrame = spark.read.json(path)
      val updatedDf: DataFrame = {
        if (parentNode != null && parentNode.trim.nonEmpty) {
          // Covered till explode of structured data frame, to work on explode of json based dataframe
          df.select(explode_outer(col(parentNode)).as(parentNode)).select(s"$parentNode.*")
        } else
          df
      }
      println("Check 1 - Source Schema")
      //  updatedDf.printSchema()
      val uuid = udf(() => java.util.UUID.randomUUID().toString)
      iterateValues(updatedDf).withColumn("EDH_UUID", uuid()).withColumn("EDH_TIMESTAMP", lit(Instant.now.getEpochSecond.toString))
    }

    // Full flattenJsonData Methods
    def fullFlattenSchema(schema: StructType): Seq[String] = {
      def helper(schema: StructType, prefix: String): Seq[String] = {
        val fullName: String => String = name => if (prefix.isEmpty) name else s"$prefix.$name"
        schema.fields.flatMap {
          case StructField(name, inner: StructType, _, _) =>
            fullName(name) +: helper(inner, fullName(name))
          case StructField(name, _, _, _) => Seq(fullName(name))
        }
      }

      helper(schema, "")
    }

    def iterateValues(updatedDf: DataFrame): DataFrame = {
      Try {
        val flattenColumns: Seq[String] = fullFlattenSchema(updatedDf.schema)
        //      println(flattenColumns.mkString("\n"))
        val flatterCols: Seq[Column] = flattenColumns.par
          .filter(_.contains("."))
          .foldLeft(Seq.empty[Column])((seq, column) => {
            seq :+ col(column).as(column.replace(".", "_"))
          }) ++ flattenColumns.filterNot(_.contains(".")).map(col)

        val columnRenamedDf: DataFrame = updatedDf.select(flatterCols: _*)

        val structNames: Seq[String] =
          columnRenamedDf.schema.filter(_.dataType.typeName == "struct").map(_.name)

        val updatedDfs: DataFrame = columnRenamedDf.drop(structNames: _*)

        val arrayColumns = updatedDfs.schema
          .filter(_.dataType.isInstanceOf[ArrayType])
          .map(_.name)

        if (arrayColumns.isEmpty) updatedDfs

        else {
          val flaternDf = arrayColumns.foldLeft(updatedDfs)((df, column) => {
            df.withColumn(column, explode_outer(col(column)))
          })

          iterateValues(
            flaternDf
          )
        }


      } match {
        case Success(value) => value
        case Failure(e) =>
          throw new Exception(e.getMessage.split("\n").take(10).mkString("\n"))
      }
    }

    // Schema Search
    def schemaWrite(schemaLocation: String, payloadSchema: StructType): DataType = {
      val jsonSchemaFileLocation = "C:\\Users\\S441633\\OneDrive - Emirates Group\\ENIT\\Code\\IssueLinksParse\\Schema\\schema.json"
      val currentDfSchema: String = payloadSchema.json
      val currentPath = new Path(jsonSchemaFileLocation)
      if (fs.exists(currentPath) && fs.listStatus(currentPath).nonEmpty) {
        fs.delete(currentPath, true)
      }
      val os: FSDataOutputStream = fs.create(currentPath)
      os.write(currentDfSchema.getBytes)
      os.close()
      val newSchema = DataType.fromJson(currentDfSchema)
      newSchema
    }

    def schemaLoader(schemaBaseLocation: String) = {
      val activeSchemaPath: Path = new Path("C:\\Users\\S441633\\OneDrive - Emirates Group\\ENIT\\Code\\IssueLinksParse\\Schema\\schema.json")
      val isSchemaFileExists: Boolean =
        fs.exists(activeSchemaPath) && fs.listStatus(activeSchemaPath).nonEmpty

      if (isSchemaFileExists) {
        val jsonSchemaValue: String = spark.read
          .text(schemaBaseLocation)
          .collect()
          .map(_.mkString)
          .mkString
        val dataType = DataType
          .fromJson(jsonSchemaValue)
          .asInstanceOf[StructType]
        dataType
      } else throw new Exception("Schema file not found")
    }

    val parentNode: String = ""
    val LoadType: String = "full"
    val flatJsonDf: DataFrame = flattenJsonData("C:\\Users\\HP EliteBook\\Downloads\\name1\\Send\\INPUT.json", parentNode.trim()).cache()
    println("Check 2")
    //    println(flatJsonDf)
    println("Json data flattening is completed")
    var ii = 0
    // Parsing Input for flattenJsonData methods
    val arrayTOStringUDF: UserDefinedFunction =
      udf[mutable.WrappedArray[String], mutable.WrappedArray[GenericRowWithSchema]](
        (in: mutable.WrappedArray[GenericRowWithSchema]) => {
          if (in != null) {
            println(in)
            println(in.getClass)
            println("Loop " + ii)
            ii = ii + 1

            Try {
              in.map((rowWithSchema: GenericRowWithSchema) => {
                def getData(rowWithSchema: GenericRowWithSchema): String = {
                  val dataFields = rowWithSchema.schema.fields
                    .map((field: StructField) => {
                      field.dataType match {
                        case _: StructType =>
                          println("check Struct, " + field.name)

                          s""""${field.name}":${
                            Try(getData(rowWithSchema.getAs(field.name)))
                              .getOrElse(rowWithSchema.getAs(field.name))
                          }"""
                        case _: ArrayType =>
                          println("check array, " + field.name)
                          s""""${field.name}":[${rowWithSchema.getAs(field.name).asInstanceOf[mutable.WrappedArray[GenericRowWithSchema]].map(getData).mkString("|")}]"""
                        case _ => s""""${field.name}":"${rowWithSchema.getAs(field.name)}""""
                      }
                    })
                    .mkString(",")
                  s"{$dataFields}"
                }

                getData(rowWithSchema)
              })
            }.getOrElse(in.asInstanceOf[mutable.WrappedArray[String]])
          } else null
        }
      )

    val arrayColumns = flatJsonDf.schema
      .filter(_.dataType.isInstanceOf[ArrayType])
      .map(_.name)

    val arrayColumnToString: Seq[Column] = arrayColumns
      .map(column => arrayTOStringUDF(col(column)).as(column))
    val nonArrayColumns: Array[Column] = flatJsonDf.columns.filterNot(arrayColumns.contains).map(col)

    val arrayTypedDf = flatJsonDf
      .select(arrayColumnToString ++ nonArrayColumns: _*)

    // display(arrayTypedDf)
    val columnList = arrayTypedDf.columns.map(column => col(column).cast(StringType))
    val flatDataDf = arrayTypedDf.select(columnList: _*)

    flatDataDf.show(false)
    val stringToSeq = udf { s: String => s.drop(1).dropRight(1).split(",") }

 /*   val flaternDf = arrayColumns.foldLeft(flatJsonDf)((df, column) => {
      df.withColumn(column, explode_outer(col(column)))
    })


    val finalDf = iterateValues(flaternDf)

    finalDf.show(false)
    // flatDataDf*/
    //    flatDataDf.printSchema()
    //    arrayTypedDf.printSchema()
    println("Check 3")

    if (LoadType.equalsIgnoreCase("full")) {
      //      flatDataDf.show(false)
      //      flatDataDf.write
      //        .format("csv")
      //        .mode("overwrite")
      //        .option("header",true)
      //        .save("C:\\Users\\S441633\\OneDrive - Emirates Group\\ENIT\\Code\\IssueLinksParse\\cyberflat\\")
      //      schemaWrite("C:\\Users\\S441633\\OneDrive - Emirates Group\\ENIT\\Code\\IssueLinksParse\\Schema\\", flatDataDf.schema)
    }

    else {
      val targetDfSchema = schemaLoader("C:\\Users\\S441633\\OneDrive - Emirates Group\\ENIT\\Code\\IssueLinksParse\\Schema\\")
      val finalSelectColumn = targetDfSchema
        .map(_.name)
        .diff(flatDataDf.columns)
        .map(column => {
          println(column)
          lit(null).cast(StringType).as(column)
        }) ++ flatDataDf.columns.map(col)

      val finaldf = flatDataDf.select(finalSelectColumn: _*)
      finaldf.write
        .format("parquet")
        .mode("overwrite")
        .save("C:\\Users\\S441633\\OneDrive - Emirates Group\\ENIT\\Code\\IssueLinksParse\\cyberflat\\")
      if (targetDfSchema != finaldf.schema)
        schemaWrite("C:\\Users\\S441633\\OneDrive - Emirates Group\\ENIT\\Code\\IssueLinksParse\\Schema\\", finaldf.schema)
    }

    //    val viewdf = spark.read.format("parquet").load("C:\\Users\\S441633\\OneDrive - Emirates Group\\ENIT\\Code\\IssueLinksParse\\cyberflat\\*.*")

    println("Data has been loaded into branded")


  }
}

