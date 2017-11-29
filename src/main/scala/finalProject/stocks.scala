package finalProject

import org.apache.spark.sql._
import scalafx.application.JFXApp
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.FXRenderer
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.Normalizer
import swiftvis2.plotting.Plot
import swiftvis2.plotting.ColorGradient
import org.apache.spark.sql.types.DoubleType

object Stocks extends JFXApp {
  val spark = SparkSession.builder.master("local[*]").getOrCreate
  import spark.implicits._
  Logger.getLogger("org").setLevel(Level.OFF)
  
  //Final month is 2016-12-12 

  val fullData = spark.read.option("header",true).csv("/users/jandres/CSCI3395/data/return-data.csv")
  //fullData.printSchema()
  fullData.filter('date >= "2017-01-01").show

  //SplitData
  //val Array(trainData, testData) = lblAssembledData.randomSplit(Array(0.8, 0.2)).map(_.cache())
  
  fullData.show()

  spark.stop()
  
}