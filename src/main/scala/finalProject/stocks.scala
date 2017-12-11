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
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.ml.regression.RandomForestRegressor

object Stocks extends JFXApp {
  val spark = SparkSession.builder.master("local[*]").getOrCreate
  import spark.implicits._
  Logger.getLogger("org").setLevel(Level.OFF)
  
  //Final month is 2016-12-12 
  
  val columnSchema = StructType(Array(
      StructField("gvkey",IntegerType),
      StructField("comp_name",StringType),
      StructField("date",StringType),
      StructField("fdate",StringType),
      StructField("price",DoubleType),
      StructField("price_1",DoubleType),
      StructField("price_2",DoubleType),
      StructField("price_3",DoubleType),
      StructField("price_4",DoubleType),
      StructField("price_5",DoubleType),
      StructField("price_6",DoubleType),
      StructField("price_7",DoubleType),
      StructField("price_8",DoubleType),
      StructField("price_9",DoubleType),
      StructField("price_10",DoubleType),
      StructField("price_11",DoubleType),
      StructField("return",DoubleType),
      StructField("return_1",DoubleType),
      StructField("return_2",DoubleType),
      StructField("return_3",DoubleType),
      StructField("return_4",DoubleType),
      StructField("return_5",DoubleType),
      StructField("return_6",DoubleType),
      StructField("return_7",DoubleType),
      StructField("return_8",DoubleType),
      StructField("return_9",DoubleType),
      StructField("return_10",DoubleType),
      StructField("return_11",DoubleType),
      StructField("range",DoubleType),
      StructField("range_1",DoubleType),
      StructField("range_2",DoubleType),
      StructField("range_3",DoubleType),
      StructField("range_4",DoubleType),
      StructField("range_5",DoubleType),
      StructField("range_6",DoubleType),
      StructField("range_7",DoubleType),
      StructField("range_8",DoubleType),
      StructField("range_9",DoubleType),
      StructField("range_10",DoubleType),
      StructField("range_11",DoubleType),
      StructField("volume",DoubleType),
      StructField("volume_1",DoubleType),
      StructField("volume_2",DoubleType),
      StructField("volume_3",DoubleType),
      StructField("volume_4",DoubleType),
      StructField("volume_5",DoubleType),
      StructField("volume_6",DoubleType),
      StructField("volume_7",DoubleType),
      StructField("volume_8",DoubleType),
      StructField("volume_9",DoubleType),
      StructField("volume_10",DoubleType),
      StructField("volume_11",DoubleType),
      StructField("sales",DoubleType),
      StructField("assets",DoubleType),
      StructField("profit_margin",DoubleType),
      StructField("asset_turnover",DoubleType),
      StructField("financial_leverage",DoubleType),
      StructField("roe",DoubleType),
      StructField("price_to_earnings",DoubleType),
      StructField("price_to_book",DoubleType),
      StructField("price_to_sales",DoubleType),
      StructField("ind_agric",DoubleType),
      StructField("ind_mines",DoubleType),
      StructField("ind_oil",DoubleType),
      StructField("ind_stone",DoubleType),
      StructField("ind_cnstr",DoubleType),
      StructField("ind_food",DoubleType),
      StructField("ind_smoke",DoubleType),
      StructField("ind_txtls",DoubleType),
      StructField("ind_apprl",DoubleType),
      StructField("ind_wood",DoubleType),
      StructField("ind_chair",DoubleType),
      StructField("ind_paper",DoubleType),
      StructField("ind_print",DoubleType),
      StructField("ind_chems",DoubleType),
      StructField("ind_ptrlm",DoubleType),
      StructField("ind_rubbr",DoubleType),
      StructField("ind_lethr",DoubleType),
      StructField("ind_glass",DoubleType),
      StructField("ind_metal",DoubleType),
      StructField("ind_mtlpr",DoubleType),
      StructField("ind_machn",DoubleType),
      StructField("ind_elctr",DoubleType),
      StructField("ind_cars",DoubleType),
      StructField("ind_instr",DoubleType),
      StructField("ind_manuf",DoubleType),
      StructField("ind_trans",DoubleType),
      StructField("ind_phone",DoubleType),
      StructField("ind_tv",DoubleType),
      StructField("ind_utils",DoubleType),
      StructField("ind_garbg",DoubleType),
      StructField("ind_stream",DoubleType),
      StructField("ind_water",DoubleType),
      StructField("ind_whlsl",DoubleType),
      StructField("ind_rtail",DoubleType),
      StructField("ind_money",DoubleType),
      StructField("ind_srvc",DoubleType),
      StructField("ind_govt",DoubleType),
      StructField("ind_other",DoubleType),
      StructField("next_return",DoubleType),
      StructField("next_return_decile",DoubleType),
      StructField("next_return_1",DoubleType),
      StructField("next_return_2",DoubleType),
      StructField("next_return_3",DoubleType),
      StructField("next_return_4",DoubleType),
      StructField("next_return_5",DoubleType),
      StructField("next_return_6",DoubleType),
      StructField("next_return_7",DoubleType),
      StructField("next_return_8",DoubleType),
      StructField("next_return_9",DoubleType),
      StructField("next_return_10",DoubleType),
      StructField("next_return_11",DoubleType)
    ))
    
   val regSchema = StructType(Array(
      StructField("price",DoubleType),
      StructField("return",DoubleType),
      StructField("sales",DoubleType),
      StructField("assets",DoubleType),
      StructField("profit_margin",DoubleType),
      StructField("roe",DoubleType),
      StructField("price_to_earnings",DoubleType),
      StructField("price_to_book",DoubleType),
      StructField("price_to_sales",DoubleType),
      StructField("next_return",DoubleType)
    ))
    
  val fullData = spark.read.schema(columnSchema).option("header",true).option("delimiter", ",").csv("/data/BigData/students/jandres/return-data.csv").orderBy("gvkey")
  
  val techBubble = fullData.filter('date >= "1995-01-01" && 'date <= "2003-12-31")
  val finCrisis = fullData.filter('date >= "2004-01-01" && 'date <= "2012-12-31")
  val oilCrisis = fullData.filter('date >= "1969-01-01" && 'date <= "1977-12-31")

  val techDataSet = techBubble.select('gvkey,'comp_name,'date,'price,'return,'sales,'assets,'profit_margin,'roe,'price_to_earnings,'price_to_book,'price_to_sales,'next_return)

  val finDataSet = finCrisis.rdd.collect()
  val oilDataSet = oilCrisis.rdd.collect()      
//  
  //convert array to make correlation matrix
  val dataInfo = techBubble.rdd.map { line =>   
    Row( //gvkey,      comp_name,              date,             price,           return,           sales,               assets,         profit_margin,         roe,                 p/e,            p/b,            price to sales,     next_return
        /*line.getInt(0),line.getString(1),line.getString(2),*/line.getDouble(5),line.getDouble(16),line.getDouble(52),line.getDouble(53),line.getDouble(54),line.getDouble(57),line.getDouble(58),line.getDouble(59),line.getDouble(60),line.getDouble(99)
    )
  }


  val dFrame = spark.createDataFrame(dataInfo, regSchema).where('sales =!= 0).filter(x => x.getDouble(4).isInfinity == false).filter(x => x.getDouble(5).isInfinity == false).filter(x => x.getDouble(8).isInfinity == false)
//  println(dFrame.where('price === 0).count())
//  println(dFrame.select('roe).filter(x => x.getDouble(0).isInfinity).count() )
//  println(dFrame.where('price.isNotNull).count)
//  println(dFrame.where('sales.isNotNull).count)
//  println(dFrame.where('roe.isNotNull).count)
//  println(dFrame.where('price.isNaN).count)
//  println(dFrame.where('sales.isNaN).count)
//  println(dFrame.where('roe.isNaN).count)
  
//  val dataVectors = dataInfo.map(x => Vectors.dense(x))
//  val correlMatrix: Matrix = Statistics.corr(dataVectors, "pearson")
  
  val techCol = "profit_margin"
  val techCol1 = "sales"
  val techCol2 = "price"
  val techVa = new VectorAssembler().setInputCols(Array(techCol1,techCol2)).setOutputCol("features")
  val techwithFeatures = techVa.transform(dFrame)
  //techwithFeatures.show(false)
  //val techlr = new RandomForestRegressor().setLabelCol(techCol)
  val techlr = new LinearRegression().setLabelCol(techCol)  //label Col as What we're looking for?
  println(techlr.explainParams())
  //println("fail")
  val techmodel = techlr.fit(techwithFeatures)
  //println(techmodel.coefficients+" "+techmodel.intercept)
  val fitDatatech = techmodel.transform(techwithFeatures).select(techCol,techCol1,techCol2,"features","prediction")
  fitDatatech.show(false)
  println("Describe tech")
  fitDatatech.describe("prediction").show()
  fitDatatech.describe(techCol).show()
  
  val x = fitDatatech.select(techCol1).as[Double].collect()
  val y = fitDatatech.select(techCol2).as[Double].collect()
  val predict = fitDatatech.select('prediction).as[Double].collect()

  val cg = ColorGradient(-1.0 -> RedARGB, 0.0-> BlueARGB, 1.0 -> GreenARGB)
  val plot = Plot.scatterPlot(x, y, "Station Clusters", techCol1, techCol2, 2, predict.map(cg))
  
  FXRenderer(plot)


  spark.stop()
  
}