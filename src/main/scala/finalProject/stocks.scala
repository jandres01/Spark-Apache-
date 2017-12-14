package finalProject

import org.apache.spark.sql._
import java.io._
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
  
  val techBubble = fullData.filter('date >= "1997-01-01" && 'date <= "2001-12-31")
  val finCrisis = fullData.filter('date >= "2006-01-01" && 'date <= "2010-12-31")
  val oilCrisis = fullData.filter('date >= "1971-01-01" && 'date <= "1975-12-31") 
  val noCrisis = fullData.filter('date >= "1985-01-01" && 'date <= "1993-12-31") 

  //convert array to make correlation matrix
  val techInfo = techBubble.rdd.map { line =>   
    Row( //gvkey,      comp_name,              date,             price,           return,           sales,               assets,         profit_margin,         roe,                 Profit Margin,            p/b,            price to sales,     next_return
        /*line.getInt(0),line.getString(1),line.getString(2),*/line.getDouble(5),line.getDouble(16),line.getDouble(52),line.getDouble(53),line.getDouble(54),line.getDouble(57),line.getDouble(58),line.getDouble(59),line.getDouble(60),line.getDouble(99)
    )
  }
  
  val fullArr = techBubble.rdd.map { line =>   
    Array( //gvkey,      comp_name,              date,             price,           return,           sales,               assets,         profit_margin,         roe,                 Profit Margin,            p/b,            price to sales,     next_return
        /*line.getInt(0),line.getString(1),line.getString(2),*/line.getDouble(5),line.getDouble(16),line.getDouble(52),line.getDouble(53),line.getDouble(54),line.getDouble(57),line.getDouble(58),line.getDouble(59),line.getDouble(60),line.getDouble(99)
    )
  }
  
  val finInfo = finCrisis.rdd.map { line =>   
    Row( //gvkey,      comp_name,              date,             price,           return,           sales,               assets,         profit_margin,         roe,                 Profit Margin,            p/b,            price to sales,     next_return
        /*line.getInt(0),line.getString(1),line.getString(2),*/line.getDouble(5),line.getDouble(16),line.getDouble(52),line.getDouble(53),line.getDouble(54),line.getDouble(57),line.getDouble(58),line.getDouble(59),line.getDouble(60),line.getDouble(99)
    )
  }
  
  val oilInfo = oilCrisis.rdd.map { line =>   
    Row( //gvkey,      comp_name,              date,             price,           return,           sales,               assets,         profit_margin,         roe,                 Profit Margin,            p/b,            price to sales,     next_return
        /*line.getInt(0),line.getString(1),line.getString(2),*/line.getDouble(5),line.getDouble(16),line.getDouble(52),line.getDouble(53),line.getDouble(54),line.getDouble(57),line.getDouble(58),line.getDouble(59),line.getDouble(60),line.getDouble(99)
    )
  }
  
  val goodInfo = noCrisis.rdd.map { line =>   
    Row( //gvkey,      comp_name,              date,             price,           return,           sales,               assets,         profit_margin,         roe,                 Profit Margin,            p/b,            price to sales,     next_return
        /*line.getInt(0),line.getString(1),line.getString(2),*/line.getDouble(5),line.getDouble(16),line.getDouble(52),line.getDouble(53),line.getDouble(54),line.getDouble(57),line.getDouble(58),line.getDouble(59),line.getDouble(60),line.getDouble(99)
    )
  }
  
  val fullInfo = fullData.rdd.map { line =>   
    Row( //gvkey,      comp_name,              date,             price,           return,           sales,               assets,         profit_margin,         roe,                 Profit Margin,            p/b,            price to sales,     next_return
        /*line.getInt(0),line.getString(1),line.getString(2),*/line.getDouble(5),line.getDouble(16),line.getDouble(52),line.getDouble(53),line.getDouble(54),line.getDouble(57),line.getDouble(58),line.getDouble(59),line.getDouble(60),line.getDouble(99)
    )
  }

  val techDFrame = spark.createDataFrame(techInfo, regSchema).where('sales =!= 0).filter(x => x.getDouble(4).isInfinity == false).filter(x => x.getDouble(5).isInfinity == false).filter(x => x.getDouble(8).isInfinity == false)
  val finDFrame = spark.createDataFrame(finInfo, regSchema).where('sales =!= 0).filter(x => x.getDouble(4).isInfinity == false).filter(x => x.getDouble(5).isInfinity == false).filter(x => x.getDouble(8).isInfinity == false)
  val oilDFrame = spark.createDataFrame(oilInfo, regSchema).where('sales =!= 0).filter(x => x.getDouble(4).isInfinity == false).filter(x => x.getDouble(5).isInfinity == false).filter(x => x.getDouble(8).isInfinity == false)
  val fullDFrame = spark.createDataFrame(fullInfo, regSchema).where('sales =!= 0).filter(x => x.getDouble(4).isInfinity == false).filter(x => x.getDouble(5).isInfinity == false).filter(x => x.getDouble(8).isInfinity == false)
  val goodDFrame = spark.createDataFrame(goodInfo, regSchema).where('sales =!= 0).filter(x => x.getDouble(4).isInfinity == false).filter(x => x.getDouble(5).isInfinity == false).filter(x => x.getDouble(8).isInfinity == false)
  
  
  val colNames = Array("price","return","sales","assets","profit_margin","roe","price_to_earnings","price_to_book","price_to_sales","next_return")
  
  val dataVectors = fullArr.map(x => Vectors.dense(x))
  val correlMatrix: Matrix = Statistics.corr(dataVectors, "pearson")
  
  /*
   * Print Correlation Matrix
   */
  
  val pw = new PrintWriter(new File("/users/jandres/CSCI3395/data/corrData.txt"))
  pw.write("price \t return \t sales \t assets \t profit_margin \t roe \t price_to_earnings \t price_to_book \t price_to_sales \t next_return \n")
  pw.write(correlMatrix.toString(10,Int.MaxValue))
  pw.close()
  println("price \t return \t sales \t assets \t profit_margin \t roe \t price_to_earnings \t price_to_book \t price_to_sales \t next_return")
  println(correlMatrix.toString(10,Int.MaxValue))
  
  def listCorrelations(searchCol:String,compCorr:Double):ListBuffer[String] = {
   val colNum = colNames.indexOf(searchCol)
   val listCorr = new ListBuffer[String]()
   
   for(i <- 0 until colNames.size) {
     if(correlMatrix.apply(i, colNum) >= compCorr && correlMatrix.apply(i, colNum) != 1.0) {
       listCorr += colNames(i)
     }
   }
   listCorr
 }
 
 //println("top correlations tech bubble profit margin "+listCorrelations("profit_margin",0.0))
  /*
  Create a Linear Regression with Data during the Tech Bubble Crisis
  */
  val colPredict = "roe"
  val colX = "sales"
  val colY = "price"
  
  val techVa = new VectorAssembler().setInputCols(Array(colX,colY)).setOutputCol("features")
  val techwithFeatures = techVa.transform(techDFrame)
  val techlr = new LinearRegression().setLabelCol(colPredict)  //label colPredict as What we're looking for?
  //println(techlr.explainParams())
  val techmodel = techlr.fit(techwithFeatures)
  val fitDatatech = techmodel.transform(techwithFeatures).select(colPredict,colX,colY,"features","prediction")
  
//  fitDatatech.describe("prediction").show()
//  fitDatatech.describe(colPredict).show()
  val x = fitDatatech.select(colX).as[Double].collect()
  val y = fitDatatech.select(colY).as[Double].collect()
  val predict = fitDatatech.select('prediction).as[Double].collect()
  
  val cg = ColorGradient(0.0 -> RedARGB, 0.20 -> GreenARGB) //profit margin cg
  //val cg = ColorGradient(40.0 -> GreenARGB, 70.00 -> RedARGB) //Profit Margin cg
  val plot = Plot.scatterPlot(x, y, colPredict+" Tech Bubble", colX, colY, 2, predict.map(cg))
  
  //FXRenderer(plot)
  
  val finVa = new VectorAssembler().setInputCols(Array(colX,colY)).setOutputCol("features")
  val finwithFeatures = finVa.transform(finDFrame)
  val finlr = new LinearRegression().setLabelCol(colPredict)  //label colPredict as What we're looking for?

  val finmodel = finlr.fit(finwithFeatures)
  val fitDatafin = finmodel.transform(finwithFeatures).select(colPredict,colX,colY,"features","prediction")
  val finX = fitDatafin.select(colX).as[Double].collect()
  val finY = fitDatafin.select(colY).as[Double].collect()
  val finPredict = fitDatafin.select('prediction).as[Double].collect()

  val finPlot = Plot.scatterPlot(finX, finY, colPredict+" Financial Crisis", colX, colY, 2, finPredict.map(cg))
  
  //FXRenderer(finPlot)
  
  val oilVa = new VectorAssembler().setInputCols(Array(colX,colY)).setOutputCol("features")
  val oilwithFeatures = oilVa.transform(oilDFrame)
  val oillr = new LinearRegression().setLabelCol(colPredict)  //label colPredict as What we're looking for?

  val oilmodel = oillr.fit(oilwithFeatures)

  val fitDataoil = oilmodel.transform(oilwithFeatures).select(colPredict,colX,colY,"features","prediction")

  val oilX = fitDataoil.select(colX).as[Double].collect()
  val oilY = fitDataoil.select(colY).as[Double].collect()
  val oilPredict = fitDataoil.select('prediction).as[Double].collect()
  //val oilcg = ColorGradient(0.0 -> RedARGB, 0.25 -> GreenARGB)
  val oilPlot = Plot.scatterPlot(oilX, oilY, colPredict+" Oil Crisis", colX, colY, 2, oilPredict.map(cg))
  
  //FXRenderer(oilPlot)
  
  val fullVa = new VectorAssembler().setInputCols(Array(colX,colY)).setOutputCol("features")
  val fullwithFeatures = fullVa.transform(fullDFrame)
  val fulllr = new LinearRegression().setLabelCol(colPredict)  //label colPredict as What we're looking for?

  val fullmodel = fulllr.fit(fullwithFeatures)

  val fitDatafull = fullmodel.transform(fullwithFeatures).select(colPredict,colX,colY,"features","prediction")

  val fullX = fitDatafull.select(colX).as[Double].collect()
  val fullY = fitDatafull.select(colY).as[Double].collect()
  val fullPredict = fitDatafull.select('prediction).as[Double].collect()

  val fullPlot = Plot.scatterPlot(fullX, fullY, colPredict+" full Data", colX, colY, 2, fullPredict.map(cg))
//  
//  FXRenderer(fullPlot)

  val goodVa = new VectorAssembler().setInputCols(Array(colX,colY)).setOutputCol("features")
  val goodwithFeatures = goodVa.transform(goodDFrame)
  val goodlr = new LinearRegression().setLabelCol(colPredict)  //label colPredict as What we're looking for?

  val goodmodel = goodlr.fit(goodwithFeatures)

  val fitDatagood = goodmodel.transform(goodwithFeatures).select(colPredict,colX,colY,"features","prediction")

  val goodX = fitDatagood.select(colX).as[Double].collect()
  val goodY = fitDatagood.select(colY).as[Double].collect()
  val goodPredict = fitDatagood.select('prediction).as[Double].collect()
  val goodPlot = Plot.scatterPlot(goodX, goodY, colPredict+" good Data", colX, colY, 2, goodPredict.map(cg))
  
  //FXRenderer(goodPlot)
  
//     val tplot = Plot.scatterPlotGrid(Seq( Seq((fullX,fullY,fullPredict.map(cg),3.0)), 
//                                           Seq((oilX,oilY,oilPredict.map(cg),3.0),(x,y,predict.map(cg),3.0)),
//                                           Seq((finX,finY,finPredict.map(cg),3.0),(goodX,goodY,goodPredict.map(cg),3.0))
//                                         ),colX,colY,"Monthly Stock Data "+colPredict)  
//  
//  FXRenderer(tplot)
//
 //FXRenderer.aliased(oilPlot, 1800, 900)
 //FXRenderer.aliased(plot, 1800, 900)  
 //FXRenderer.aliased(finPlot, 1800, 900)
 //FXRenderer.aliased(goodPlot, 1800, 900)
  FXRenderer.aliased(fullPlot, 1800, 900)
  
  spark.stop()
  
}