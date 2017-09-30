package sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object sparkSQL extends App {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  val schema = StructType(Array(
    StructField("series_id", StringType),
    StructField("year", IntegerType),
    StructField("period", StringType),
    StructField("value", DoubleType),
    StructField("footnote_codes", StringType)))

  val dataNM = spark.read.schema(schema).option("header", true).option("delimiter", "\t").
    csv("/data/BigData/bls/la/la.data.38.NewMexico")

  val dataTX = spark.read.schema(schema).option("header", true).option("delimiter", "\t").
    csv("/data/BigData/bls/la/la.data.51.Texas")
    
  val schema2 = StructType(Array(
    StructField("series_id", StringType),
    StructField("area_type_code", StringType),
    StructField("area_code", StringType),
    StructField("measure_code", StringType),
    StructField("seasonal", StringType),
    StructField("srd_code", StringType),
    StructField("series_title", StringType),
    StructField("footnote_codes", StringType),
    StructField("begin_year", IntegerType),
    StructField("begin_period", StringType),
    StructField("end_year", IntegerType),
    StructField("end_period", StringType)))

  val dataSeries = spark.read.schema(schema2).option("header", true).option("delimiter", "\t").
    csv("/data/BigData/bls/la/la.series")

  //dataNM.show

  // IC Q1
  val q1 = dataNM.select('series_id).distinct().count()
  //println("Q1: " + q1)

  // IC Q2
  val q2 = dataNM.filter(substring('series_id, 19, 2) === "04").agg(max('value))
  println("Q2: ")
  //q2.show()

  // IC Q3
  val dsG = dataSeries.filter('area_type_code === "G")
  val q3 = dsG.as('ds).join(dataNM.as('nm), $"nm.series_id" === $"ds.series_id").count()
  //println("Q3: There are "+ q3)
  
  //IC Q4a
  val urNMID = dataNM.filter(substring('series_id, 19, 2) === "03").where('year === 2017)
  val q4a = urNMID.sort('period).groupBy('period)agg(avg('value))
  println("Q4a: ")
  //q4a.show()
  
  //IC Q4b
  val q4b = urNMID.sort(substring('series_id, 4,15)).groupBy(substring('series_id, 4,15)).agg(avg('value))
  println("Q4b: ")
 // q4b.show()
  
  //OC Q1c
 
  //val lfID = dataSeries.filter(substring('series_id, 19, 2) === "06")
  //val lfNM = lfID.as('id).join(urNMID.as('nm), $"id.series_id" === $"nm.series_id").groupBy($"id.area_code",$"id.begin_period").agg(avg($"nm.value"))
 
  
  // println("weighted average each counties in New Mexico: ")
  // lfNM.show()
  
  //OC Q2
  val lfTX = dataTX.filter(substring('series_id,19,2) === "06").where('value > 10000)
  val urTX = dataTX.filter(substring('series_id,19,2) === "03")
  val maxurTX = lfTX.as('lf).join(urTX.as('ur), $"ur.year" === $"lf.year").sort($"ur.value".desc)
  
  println("Out of Class Q2: ")
  maxurTX.show(40,false)

  
  spark.stop()
}
