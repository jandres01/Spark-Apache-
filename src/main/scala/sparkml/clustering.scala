package sparkml

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

object Clustering extends JFXApp {
  val spark = SparkSession.builder.master("local[*]").getOrCreate
  import spark.implicits._
  Logger.getLogger("org").setLevel(Level.OFF)
  // bexar county 48029

  val mainData = spark.read.option("header",true).csv("/data/BigData/bls/qcew/2016.q1-q4.singlefile.csv")
  val electionData = spark.read.option("header", true).csv("/data/BigData/bls/2016_US_County_Level_Presidential_Results.csv")
//  // in class 1
//  mainData.filter('agglvl_code >= 70 && 'agglvl_code <= 78).groupBy('agglvl_code).count().show()
//
//  // 2
//  println(mainData.filter('area_fips === 48029).count())
//
//  // 3
//  mainData.groupBy('industry_code).count().orderBy('count.desc).show(3)
//
//  // 4
//  mainData.filter('agglvl_code === 78).groupBy('industry_code).agg(sum('total_qtrly_wages).as("sum")).orderBy('sum.desc).show(3)

  //Out of Class Q#1
  
  val generateZip2 = udf((row:String) => if(row.size == 4) 0+row else if (row.size == 3) 0+(0+row) else row)
  val convertInt = udf((row:String) => if(row.contains("C") || row.contains("S")) row.drop(1).toInt else row.toInt)
  val convertNormTo2Cluster = udf((row:Double) => if(row < 0.5) 0 else 1)
  val convertNormTo3Cluster = udf((row:Double) => if(row <= 0.4) 0 else if(row >=0.6) 2 else 1)
  val checkAccuracy = udf((leftData:Integer,rightData:Integer) => if(leftData == rightData) 1 else 0)
  
  val columnsToKeep2 = "avg_wkly_wage lq_qtrly_estabs oty_total_qtrly_wages_chg".split(" ") 
  val typedData2 = columnsToKeep2.foldLeft(mainData)((df, colName) => df.withColumn(colName, df(colName).cast(IntegerType).as(colName))).drop(mainData.col("disclosure_code")).drop(mainData.col("lq_disclosure_code")).drop(mainData.col("oty_disclosure_code")).na.drop()
  val assembler = new VectorAssembler().setInputCols(columnsToKeep2).setOutputCol("features")
  val dataWithFeatures = assembler.transform(typedData2)
  //dataWithFeatures.show()

  val normalizer = new Normalizer().setInputCol("features").setOutputCol("normFeatures")
  val normData = normalizer.transform(dataWithFeatures)
  //normData.show(false)
  
  val normValues = normData.select('normFeatures)
  
  //number of clusters
  val kmeans = new KMeans().setK(3).setFeaturesCol("normFeatures")
  val model = kmeans.fit(normData)
  
  val predictions = model.transform(normData)
  val convertedPredictions =  predictions.withColumn("int_area_fips", col("area_fips").cast(IntegerType))
  val convertedElections =  electionData.withColumn("int_combined_fips", col("combined_fips").cast(IntegerType) ).withColumn("norm_cluster", convertNormTo2Cluster(col("per_dem")))
  val joinPredictElect = convertedPredictions.join(convertedElections, convertedElections.col("int_combined_fips") === convertedPredictions.col("int_area_fips"))
    
  val accuracyColumn = joinPredictElect.select('prediction,'norm_cluster).withColumn("accuracy",checkAccuracy(col("prediction"),col("norm_cluster")))
  val accuracy = accuracyColumn.filter('accuracy === 1).count().toDouble / accuracyColumn.count().toDouble

  println("The accuracy of the chosen columns prediction is "+accuracy+"%")
  
  //Out of class Q#2

  //val aggEData = electionData.filter('state_abbr =!= "AK").groupBy('county_name,'state_abbr,'votes_dem,'votes_gop).agg(avg('combined_fips).alias("combined_fips"))
  val aggEData = joinPredictElect.filter('state_abbr =!= "AK").groupBy('county_name,'state_abbr,'prediction.alias("model_prediction")).agg(avg('votes_dem).alias("votes_dem"),avg('votes_gop).alias("votes_gop"))
  val zipcodes = spark.read.option("header", true).csv("/data/BigData/bls/zip_codes_states.csv")
  val aggZip = zipcodes.filter('state =!= "AK").groupBy('county,'state).agg(avg('latitude).alias("latitude"),avg('longitude).alias("longitude"))
  val electionZip = aggEData.join(aggZip,(aggEData.col("county_name").contains($"county")))
  
  val columnsToKeep = "latitude longitude".split(" ")

  val typedData = columnsToKeep.foldLeft(electionZip)((df, colName) => df.withColumn(colName, df(colName).cast(DoubleType).as(colName))).na.drop()
  val stationVA = new VectorAssembler().setInputCols(columnsToKeep).setOutputCol("location")
  val stationsWithVect = stationVA.transform(typedData) 
  val kMeans = new KMeans().setK(3).setFeaturesCol("location")
  val stationClusterModel = kMeans.fit(stationsWithVect)
  
  val stationsWithClusters = stationClusterModel.transform(stationsWithVect)
  //stationsWithClusters.show()
  val x = stationsWithClusters.select('longitude).as[Double].collect()
  val y = stationsWithClusters.select('latitude).as[Double].collect()
  val predict = stationsWithClusters.select('model_prediction).as[Double].collect()

  val cg = ColorGradient(0.0 -> BlueARGB, 0.4-> GreenARGB, 0.6 -> RedARGB)
  val plot = Plot.scatterPlot(x, y, "Station Clusters", "Longitude", "Latitude", 2, predict.map(cg))
  
  FXRenderer(plot)

  spark.stop()
  
}
