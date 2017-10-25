package udf


import scalafx.application.JFXApp
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.FXRenderer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.expressions.Aggregator

//spark-submit --class ClassSQL --master spark://pandora00:7077 target/scala-2.11/CSCI3395-F17-InClassLewis-assembly-0.1.0-SNAPSHOT.jar

object UDF extends JFXApp {
  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  case class electionSchema(id:Int,votes_dem:Double,votes_gop:Double,total_votes:Double,per_dem:Double,per_gop:Double,
 diff:Double,per_point_diff:String,state_abbr:String,county_name:String,combined_fips:Int)

case class zipSchema(zip_code:String,latitude:Double,longitude:Double,city:String,state:String,county:String) 

case class stateSchema(series_id:String,year:Int,period:String,value:Double)

case class areaSchema(area_type_code:String,area_code:String,area_text:String)

 val data = spark.read.schema(Encoders.product[electionSchema].schema).
    option("header", true).
    csv("/data/BigData/bls/2016_US_County_Level_Presidential_Results.csv").
    as[electionSchema]
val zipdata = spark.read.schema(Encoders.product[zipSchema].schema).
    option("header", true).
    csv("/data/BigData/bls/zip_codes_states.csv").
    as[zipSchema]
val statesData = spark.read.schema(Encoders.product[stateSchema].schema).
    option("header", true).option("delimiter", "\t").
    csv("/data/BigData/bls/la/la.data.concatenatedStateFiles").
    as[stateSchema]
val areaData = spark.read.schema(Encoders.product[areaSchema].schema).
    option("header", true).option("delimiter", "\t").
    csv("/data/BigData/bls/la/la.area").
    as[areaSchema]



println("\n")
 println("In Class")
println("")

 val electResults= data.filter(s=>s.state_abbr != "AK")
 //electResults.show(20)

 val totalRepublicanCounty = electResults.filter(s=> s.votes_gop > s.votes_dem).count()
 val totalCounty = electResults.count()
 println("1.) " + totalRepublicanCounty + "/" + totalCounty + " | " + totalRepublicanCounty.toDouble/totalCounty.toDouble)
val repub10 = electResults.filter(s=> ((s.votes_gop.toDouble/(s.votes_dem.toDouble+s.votes_gop.toDouble))-(s.votes_dem.toDouble/(s.votes_dem.toDouble+s.votes_gop.toDouble))) > 0.1).count
val dem10 = electResults.filter(s=> ((s.votes_dem.toDouble/(s.votes_dem.toDouble+s.votes_gop.toDouble))-(s.votes_gop.toDouble/(s.votes_dem.toDouble+s.votes_gop.toDouble))) > 0.1).count
println("2.A) Republican: " + repub10 + "/" + totalCounty + " | " + repub10.toDouble/totalCounty.toDouble)
println("2.B) Democrats: " + dem10 + "/" + totalCounty + " | " + dem10.toDouble/totalCounty.toDouble)

println("3.)")


val totalDemocrats = electResults.map(s=>s.votes_dem).collect
val totalVotes = electResults.map(s=>s.total_votes).collect
val totalPercent = electResults.map(s=>s.votes_dem.toDouble/s.total_votes.toDouble).collect


val cg2 = ColorGradient((0.40, RedARGB), (0.60, BlueARGB))
val plot = Plot.scatterPlot(totalVotes,totalPercent,"2016 Democratic Party","# of Votes","% Democrat",4,totalDemocrats.map(cg2))
FXRenderer(plot,1280,720)




println("4.)")
//zipdata.show()
//electResults.show()
//val electionZip = zipdata.dropDuplicates(Seq("state","county")).join(electResults,($"county_name".contains($"county") && $"state" === $"state_abbr")).filter($"longitude" > -145.00)
val electionZip = zipdata.join(electResults,($"county_name".contains($"county") && $"state" === $"state_abbr")).filter($"longitude" > -145.00)
//println(electionZip.count())
//electionZip.show()
val lon = electionZip.select($"longitude").filter($"longitude".isNotNull).collect.map(s=>s.getDouble(0))
val lat = electionZip.select($"latitude").filter($"latitude".isNotNull).collect.map(s=>s.getDouble(0))
val electRes = electionZip.select($"votes_dem"/$"total_votes").collect.map(s=>s.getDouble(0))
//for(i <- 0 to 15) println(electRes(i))
val cg = ColorGradient((0.40, RedARGB), (0.60, BlueARGB))
val sPlot = Plot.scatterPlot(lon,lat,"2016 Election Results","Longitude","Latitude",3,electRes.map(cg))
FXRenderer(sPlot,1280,720)

println("")
println("Between Class:")
println("")
println("1.)")

val statesRate = statesData.filter(substring($"series_id",19,2) === "03")
/*
val statesRate1990 = statesRate.filter(s=>(s.year == 1990 && s.period == "M06")  )
val statesRate1991 = statesRate.filter( s=> (s.year == 1991 && s.period == "M03") )
statesRate1990.show()

val metroAreas = areaData.filter($"area_type_code" === "B")
val microAreas = areaData.filter($"area_type_code" === "D")
val countyAreas = areaData.filter($"area_type_code" === "F")

val joinedMetroRate1990 = statesRate1990.join(metroAreas, $"series_id".contains($"area_code"))
joinedMetroRate1990.show()

val joinedMetroRate1991 = statesRate1991.join(metroAreas, $"series_id".contains($"area_code"))

 val bins = (0.0 to 50.0 by 1.0).toArray
 val hist1990 = joinedMetroRate1990.select('value).rdd.map(s=>s.getDouble(0)).histogram(bins, true)
 val plotM1990 = Plot.histogramPlot(bins, hist1990, BlueARGB, false, "Metro 1990", "Value", "Count")
    FXRenderer(plotM1990, 500, 300)
val hist1991 = joinedMetroRate1991.select('value).rdd.map(s=>s.getDouble(0)).histogram(bins, true)
val plotM1991 = Plot.histogramPlot(bins, hist1991, BlueARGB, false, "Metro 1991", "Value", "Count")
    FXRenderer(plotM1991, 500, 300)
*/
/*
println("2.)")

electResults.show()
val stateWithArea = statesRate.join(areaData.filter($"area_type_code" === "F"), $"series_id".contains($"area_code"))
stateWithArea.show()
val totalJoin = stateWithArea.join(electResults,$"area_text".contains(concat($"county_name",lit("County, "),$"state_abbr")))
totalJoin.show()
*/
println("\n")
  spark.stop()
}