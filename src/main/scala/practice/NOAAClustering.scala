package practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import scalafx.application.JFXApp
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.FXRenderer

case class NOAAData(sid: String, date: java.sql.Date, measure: String, value: Double)
case class Station(sid: String, lat: Double, lon: Double, elev: Double, name: String)

object NOAAClustering extends JFXApp {
  val spark = SparkSession.builder.appName("NOAA SQL Data").master("local[*]").getOrCreate()
  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

//  val data2017 = spark.read.schema(Encoders.product[NOAAData].schema).option("dateFormat", "yyyyMMdd").
//    csv("data/NOAA/2017.csv").as[NOAAData].cache()

  val stations = spark.read.textFile("/users/mlewis/CSCI3395-F17/data/NOAA/ghcnd-stations.txt").map { line =>
    val id = line.substring(0, 11)
    val lat = line.substring(12, 20).trim.toDouble
    val lon = line.substring(21, 30).trim.toDouble
    val elev = line.substring(31, 37).trim.toDouble
    val name = line.substring(41, 71).trim
    Station(id, lat, lon, elev, name)
  }.cache()
  
  val stationVA = new VectorAssembler().setInputCols(Array("lat","lon")).setOutputCol("location")
  val stationsWithVect = stationVA.transform(stations) 

  val kMeans = new KMeans().setK(2000).setFeaturesCol("location")
  val stationClusterModel = kMeans.fit(stationsWithVect)
  
  val stationsWithClusters = stationClusterModel.transform(stationsWithVect)
  stationsWithClusters.show
  
  val x = stationsWithClusters.select('lon).as[Double].collect()
  x.take(20) foreach println
  val y = stationsWithClusters.select('lat).as[Double].collect()
  println("y")
  y.take(20) foreach println
  val predict = stationsWithClusters.select('prediction).as[Double].collect()
  println("predict")
  predict.take(20) foreach println
  
  val cg = ColorGradient(0.0 -> BlueARGB, 1000.0 -> RedARGB, 2000.0 -> GreenARGB)
  val plot = Plot.scatterPlot(x, y, "Station Clusters", "Longitude", "Latitude", 3, predict.map(cg))
  
  FXRenderer(plot)
  
  spark.stop()
}