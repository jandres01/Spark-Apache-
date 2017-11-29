package sparkml

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer

object regression extends App {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._
  Logger.getLogger("org").setLevel(Level.OFF)
  
  val columnSchema = StructType(Array(
      StructField("starting_col",IntegerType),
      StructField("variable_name",StringType),
      StructField("field_length",IntegerType)))
  
  val odata = spark.read.schema(columnSchema).option("header",true).option("delimiter", "\t").csv("/data/BigData/brfss/Columns.txt").orderBy("starting_col")
  val dataSet = odata.rdd.collect()
  
  val columnData = dataSet.map{e =>
    (e.getString(1),e.getInt(0),e.getInt(2))
  }
  
  //columnData.take(5) foreach println

  val dataRDD = spark.sparkContext.textFile("/data/BigData/brfss/LLCP2016.asc")
  
  //dataRDD.take(5) foreach println
  
  val dataInfo = dataRDD.map { line =>
    Row(columnData.map { row =>
      //line.substring(row._2 - 1, row._2 + row._3).toDouble
      try {
        line.substring(row._2,row._2 + row._3).toDouble
      } catch {
        case _:NumberFormatException => -1
      }
      //if(!line.substring(row._2,row._2 + row._3).trim().isEmpty()) line.substring(row._2,row._2 + row._3).trim().toDouble else 0
    }:_*)   //:_* is everything
  }
  
  val dfSchema = StructType(dataSet.map(e => StructField(e.getString(1),DoubleType)))
  
  val dFrame = spark.createDataFrame(dataInfo, dfSchema)
  
  println("In Class A:")
  dFrame.describe("GENHLTH").show()
  println("In Class B:")
  dFrame.describe("PHYSHLTH").show()
  println("In Class C:")
  dFrame.describe("MENTHLTH").show()
  println("In Class D:")
  dFrame.describe("POORHLTH").show()
  println("In Class E:")
  dFrame.describe("EXERANY2").show()
  println("In Class F:")
  dFrame.describe("SLEPTIM1").show()
  
  val dataDoubles = dataRDD.map { line =>
    Array(columnData.map { row =>
      try {
        line.substring(row._2,row._2 + row._3).toDouble
      } catch {
        case _:NumberFormatException => -1
      }
    }:_*)   //:_* is everything 
  }
  
 
  
 val colNames = columnData.map(x => x._1)
  
 val dataVectors = dataDoubles.map(x => Vectors.dense(x))
 
 val correlMatrix: Matrix = Statistics.corr(dataVectors, "pearson")
 //println(correlMatrix.toString)
 
 // +1 perfect correlation & -1 is perfect correlation but opposite direction
 def listCorrelations(searchCol:String,compCorr:Double):ListBuffer[String] = {
   val colNum = colNames.indexOf(searchCol)
   val listCorr = new ListBuffer[String]()
   
   for(i <- 0 until colNames.size) {
     if(correlMatrix.apply(i, colNum) > compCorr && correlMatrix.apply(i, colNum) != 1.0) {
       listCorr += colNames(i)
     }
   }
   listCorr
 }
 
 println("Gen list "+listCorrelations("GENHLTH",0.12))
 println("Phys list "+listCorrelations("PHYSHLTH",0.15))
 println("Ment list "+listCorrelations("MENTHLTH",0.08))
 println("Poor list "+listCorrelations("POORHLTH",0.06))
 
  //Vector Assembler for GENHLTH
  val genCol = "GENHLTH"
  val genCol1 = "HAVARTH3"
  val genCol2 = "_ASTHMS1"
  val genVa = new VectorAssembler().setInputCols(Array(genCol1,genCol2)).setOutputCol("features")
  val genwithFeatures = genVa.transform(dFrame)
  val genlr = new LinearRegression().setLabelCol(genCol)  //label Col as What we're looking for?
  val genmodel = genlr.fit(genwithFeatures)
  //println(genmodel.coefficients+" "+genmodel.intercept)
  val fitDataGen = genmodel.transform(genwithFeatures).select(genCol,genCol1,genCol2,"features","prediction")
  //fitDataGen.show()
  println("Describe Gen")
  //fitDataGen.describe("prediction").show()

  val physCol = "PHYSHLTH"
  val physCol1 = "HAVARTH3"
  val physCol2 = "CHCCOPD1"
  val physVa = new VectorAssembler().setInputCols(Array(physCol1,physCol2)).setOutputCol("features")
  val physwithFeatures = physVa.transform(dFrame)
  val physlr = new LinearRegression().setLabelCol(physCol)  //label Col as What we're looking for?
  val physmodel = physlr.fit(physwithFeatures)
  //println(physmodel.coefficients+" "+physmodel.intercept)
  val fitDataphys = physmodel.transform(physwithFeatures).select(physCol,physCol1,physCol2,"features","prediction")
  //fitDataphys.show()
  println("Describe PHYS")
  //fitDataphys.describe("prediction").show()
  
  val mentCol = "MENTHLTH"
  val mentCol1 = "VETERAN3"
  val mentCol2 = "_RFHLTH"
  val mentVa = new VectorAssembler().setInputCols(Array(mentCol1,mentCol2)).setOutputCol("features")
  val mentwithFeatures = mentVa.transform(dFrame)
  val mentlr = new LinearRegression().setLabelCol(mentCol)  //label Col as What we're looking for?
  val mentmodel = mentlr.fit(mentwithFeatures)
  //println(mentmodel.coefficients+" "+mentmodel.intercept)
  val fitDatament = mentmodel.transform(mentwithFeatures).select(mentCol,mentCol1,mentCol2,"features","prediction")
  //fitDatament.show()
  println("Describe MENT")
  fitDatament.describe("prediction").show()
  
  val pootCol = "POORHLTH"
  val pootCol1 = "RMVTETH3"
  val pootCol2 = "_RFHLTH"
  val pootVa = new VectorAssembler().setInputCols(Array(pootCol1,pootCol2)).setOutputCol("features")
  val pootwithFeatures = pootVa.transform(dFrame)
  val pootlr = new LinearRegression().setLabelCol(pootCol)  //label Col as What we're looking for?
  val pootmodel = pootlr.fit(pootwithFeatures)
  //println(pootmodel.coefficients+" "+pootmodel.intercept)
  val fitDatapoot = pootmodel.transform(pootwithFeatures).select(pootCol,pootCol1,pootCol2,"features","prediction")
  //fitDatapoot.show()
  println("Describe POOR")
  fitDatapoot.describe("prediction").show()
  
  spark.stop()

}
