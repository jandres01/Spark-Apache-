package sparksql

import swiftvis2.plotting._
import swiftvis2.plotting.renderer.FXRenderer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import scalafx.application.JFXApp

object sparkSQL extends JFXApp {
  val spark = SparkSession.builder().master("spark://pandora00:7077").getOrCreate()
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
    
  val states = spark.read.schema(schema).option("header", true).option("delimiter", "\t").
    csv("/data/BigData/bls/la/la.data.concatenatedStateFiles")
    
  def readFile(stNum:Int, stName:String) = {
    val data = spark.read.schema(schema).option("header", true).option("delimiter", "\t").
       csv("/data/BigData/bls/la/la.data."+stNum+"."+stName)
    data
  }  

    
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
    
  val schema3 = StructType(Array(
    StructField("zip_code", DoubleType),
    StructField("latitude", DoubleType),
    StructField("longitude", DoubleType),
    StructField("city", StringType),
    StructField("state", StringType),
    StructField("county", StringType)))

  val dataSeries = spark.read.schema(schema2).option("header", true).option("delimiter", "\t").
    csv("/data/BigData/bls/la/la.series")
    
  val dataCounty = spark.read.schema(schema).option("header", true).option("delimiter", "\t").
    csv("/data/BigData/bls/la/la.data.64.County")

 val zipSeries = spark.read.schema(schema3).option("header", true).option("delimiter", ",").
    csv("/data/BigData/bls/zip_codes_states.csv")

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
  println("Q3: There are "+ q3)
  
  //IC Q4a
  val urNMID = dataNM.filter(substring('series_id, 19, 2) === "03").where('year === 2017)
  val q4a = urNMID.sort('period).groupBy('period).agg(avg('value).alias("avg_v"))
  val avgM = q4a.agg(avg('avg_v))
  println("Q4a: ")
  //avgM.show()
  
  //IC Q4b
  val q4b = urNMID.sort(substring('series_id, 4,15)).agg(avg('value))
  println("Q4b: ")
  q4b.show()
  
  //OC Q1c
  println("Out of Class Q1: ")
  dataNM.createOrReplaceTempView("nmdata")
  val lf = dataNM.filter(substring('series_id,19,2) === "06" && 'year === "2017").withColumn("seriesID", substring('series_id,0,18)).withColumnRenamed("value", "lfvalue")
  val wa = dataNM.filter(substring('series_id,19,2) === "03" && 'year === "2017").withColumn("seriesID", substring('series_id,0,18)).join(lf, Seq("seriesID","period")).agg(sum('value*'lfvalue)/sum('lfvalue))
 // wa.show()
  
  //Out of Class Q2
  dataTX.createOrReplaceTempView("txdata")
  val maxURTX = spark.sql("select * from txdata AS tx,(select substring(series_id,4,15) AS lf from txdata where substring(series_id,19,2) == '06' AND value > 10000) As lfdata WHERE substring(tx.series_id,4,15)== lfdata.lf AND substring(tx.series_id,19,2) == '03' ORDER BY tx.value DESC LIMIT 1")
  //val lfTX = dataTX.filter(substring('series_id,19,2) === "06").where('value > 10000)
  // val urTX = dataTX.filter(substring('series_id,19,2) === "03")
  //  val maxurTX = lfTX.as('lf).join(urTX.as('ur), $"ur.series_id" === $"lf.series_id").sort($"ur.value".desc)
  println("Out of Class Q2: ")
 // maxURTX.show(false)
  
  //Out of Class Q3
  
  states.createOrReplaceTempView("sdata") //states loads full data set

  val highUR = spark.sql("select * from sdata AS s,(select substring(series_id,4,15) AS lf from sdata where substring(series_id,19,2) == '06' AND value > 10000) AS lfdata WHERE substring(s.series_id,4,15)== lfdata.lf AND substring(s.series_id,19,2) == '03' ORDER BY s.value DESC LIMIT 1")
  println("Out of Class Q3: ")
  //highUR.show(false)
  
  //Out of Class Q4
  dataSeries.createOrReplaceTempView("seriesD")
  println("Out of Class Q4 ")
  val highSeries = spark.sql("select srd_code,count(series_id) AS c from seriesD GROUP BY srd_code ORDER BY c DESC LIMIT 1")
 // highSeries.show()
  
//  distinctSeries.show()
  
  //Out of CLass Q5
  zipSeries.createOrReplaceTempView("zdata")
  println("Out of Class Q5")
//  val d2000 = spark.sql("select * from zdata,(select series_id, avg(sd.va), substring_index(substring_index(series_title,':',-1), 'County,' ,1) AS st from seriesD, (select series_id AS sy,value AS va from sdata where year == '2000') AS sd WHERE seriesD.series_id == sd.sy AND srd_code != '80' AND srd_code != '02' AND srd_code != '15' AND srd_code != '72' AND area_type_code ='F' GROUP BY series_id,st) AS info WHERE TRIM(info.st) == zdata.county ORDER BY county")
//  println("2000 data")
//  d2000.show()
//  
//  val d2005 = spark.sql("select * from zdata,(select series_id, avg(sd.va), substring_index(substring_index(series_title,':',-1), 'County,' ,1) AS st from seriesD, (select series_id AS sy,value AS va from sdata where year == '2005') AS sd WHERE seriesD.series_id == sd.sy AND srd_code != '80' AND srd_code != '02' AND srd_code != '15' AND srd_code != '72' AND area_type_code ='F' GROUP BY series_id,st) AS info WHERE TRIM(info.st) == zdata.county ORDER BY county")
//  println("2005 data")
//  d2005.show()
//  
//  val d2010 = spark.sql("select * from zdata,(select series_id, avg(sd.va), substring_index(substring_index(series_title,':',-1), 'County,' ,1) AS st from seriesD, (select series_id AS sy,value AS va from sdata where year == '2010') AS sd WHERE seriesD.series_id == sd.sy AND srd_code != '80' AND srd_code != '02' AND srd_code != '15' AND srd_code != '72' AND area_type_code ='F' GROUP BY series_id,st) AS info WHERE TRIM(info.st) == zdata.county ORDER BY county")
//  println("2010 data")
//  d2010.show()
 
  val d2015 = spark.sql("select * from zdata,(select series_id, avg(sd.va), substring_index(substring_index(series_title,':',-1), 'County,' ,1) AS st from seriesD, (select series_id AS sy,value AS va from sdata where year == '2015') AS sd WHERE seriesD.series_id == sd.sy AND srd_code != '80' AND srd_code != '02' AND srd_code != '15' AND srd_code != '72' AND area_type_code ='F' GROUP BY series_id,st) AS info WHERE TRIM(info.st) == zdata.county AND zdata.longitude IS NOT NULL AND zdata.latitude IS NOT NULL ORDER BY county") //) AS info WHERE TRIM(info.st) == zdata.county
  println("2015 data")
  d2015.show(false)
  
  //.cache does not reload file because storing it in memory
  
  val temps = d2015.collect()
  val c = temps.map{r => r.toSeq.toArray}
  c.take(5) foreach println
  
  val lat = c.map(a => a(1).toString().toDouble)
  val long = c.map(a => a(2).toString().toDouble)
  
  println(long(0))
  val tplot = Plot.scatterPlot(lat, long, "Temps", "year", "Temp", 10, 0xff55aa99)
  FXRenderer(tplot)
  
  //val check = spark.sql("select LOCATE('County,',substring_index(series_title,':',-1)) from seriesD where area_type_code='F'") 
  //check.show()
//  val test = spark.sql("select substring_index(substring_index(series_title,':',-1), 'County,' ,1) AS st from seriesD where area_type_code='F'") //IF(LOCATE('County,',substring_index(series_title,':',-1)), 'County,', 'Municipio,')
//  test.show(false)
  
  spark.stop()
} 
