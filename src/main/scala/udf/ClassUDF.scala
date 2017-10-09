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
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics

//spark-submit --class ClassSQL --master spark://pandora00:7077 target/scala-2.11/CSCI3395-F17-InClassLewis-assembly-0.1.0-SNAPSHOT.jar

object ClassUDF extends JFXApp {
  val spark = SparkSession.builder().master("spark://pandora00:7077").getOrCreate()
  
  val sc = spark.sparkContext
  
  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  val electionSchema = StructType(Array(
    StructField("id", IntegerType),
    StructField("votes_dem", DoubleType),
    StructField("votes_gop", DoubleType),
    StructField("total_votes", DoubleType),
    StructField("per_dem", DoubleType),
    StructField("per_gop", DoubleType),
    StructField("diff", DoubleType),
    StructField("per_point_diff", StringType),
    StructField("state_abbr", StringType),
    StructField("county_name", StringType),
    StructField("combined_flips", IntegerType)))  
    
  val zipSchema = StructType(Array(
    StructField("zip_code", DoubleType),
    StructField("latitude", DoubleType),
    StructField("longitude", DoubleType),
    StructField("city", StringType),
    StructField("state", StringType),
    StructField("county", StringType)))  
    
    
  val areaSchema = StructType(Array(
    StructField("area_type_code", StringType),
    StructField("area_code", StringType),
    StructField("area_text", StringType),
    StructField("display_level", IntegerType),
    StructField("selectable", StringType),
    StructField("sort_sequence", StringType))) 
    
  val schema = StructType(Array(
    StructField("series_id", StringType),
    StructField("year", IntegerType),
    StructField("period", StringType),
    StructField("value", DoubleType),
    StructField("footnote_codes", StringType)))

  val data = spark.read.schema(electionSchema).
    option("header", true).option("delimiter", ",").
    csv("/data/BigData/bls/2016_US_County_Level_Presidential_Results.csv")
   
 val zipdata = spark.read.schema(zipSchema).option("header", true).option("delimiter", ",").
    csv("/data/BigData/bls/zip_codes_states.csv")
    
  val statesData = spark.read.schema(schema).option("header", true).option("delimiter", "\t").
    csv("/data/BigData/bls/la/la.data.concatenatedStateFiles")
    
  val areaData = spark.read.schema(areaSchema).
    option("header", true).option("delimiter", "\t").
    csv("/data/BigData/bls/la/la.area")

  println("\n")
  println("In Class")
  println("")
  
  val electResults= data.filter('state_abbr =!= "AK")
  val totalRepublicanCounty = electResults.filter('votes_gop >= 'votes_dem).count()
  val totalCounty = electResults.count()
  println("1.) " + totalRepublicanCounty + "/" + totalCounty + " | " + totalRepublicanCounty.toDouble/totalCounty.toDouble)
  
  val repub10 = electResults.filter((('votes_gop/('votes_dem + 'votes_gop))-('votes_dem/('votes_dem + 'votes_gop))) > 0.1).count
  val dem10 = electResults.filter((('votes_dem/('votes_dem + 'votes_gop))-('votes_gop/('votes_dem + 'votes_gop))) > 0.1).count
  println("2.A) Republican: " + repub10 + "/" + totalCounty + " | " + repub10.toDouble/totalCounty.toDouble)
  println("2.B) Democrats: " + dem10 + "/" + totalCounty + " | " + dem10.toDouble/totalCounty.toDouble)

  println("3.)")

  val totalDemocrats = electResults.filter('votes_dem.isNotNull && 'total_votes.isNotNull).collect().map(s=>s.getDouble(1))
  val totalVotes = electResults.filter('votes_dem.isNotNull && 'total_votes.isNotNull).collect().map(s=>s.getDouble(3))
  val totalPercent = electResults.filter('votes_dem.isNotNull && 'total_votes.isNotNull).collect().map(s=>s.getDouble(1)/s.getDouble(3))
  

  val cg2 = ColorGradient((0.40, RedARGB), (0.60, BlueARGB))
  val plot = Plot.scatterPlot(totalVotes,totalPercent,"2016 Democratic Party","# of Votes","% Democrat",4,totalDemocrats.map(cg2))
  //FXRenderer(plot,1280,720)

  println("4.)")

  //val electionZip = zipdata.dropDuplicates(Seq("state","county")).join(electResults,($"county_name".contains($"county") && $"state" === $"state_abbr")).filter($"longitude" > -145.00)
  val electionZip = zipdata.join(electResults,($"county_name".contains($"county") && $"state" === $"state_abbr")).filter($"longitude" > -145.00)

  val lon = electionZip.select($"longitude").filter($"longitude".isNotNull).collect.map(s=>s.getDouble(0))
  val lat = electionZip.select($"latitude").filter($"latitude".isNotNull).collect.map(s=>s.getDouble(0))
  val electRes = electionZip.select($"votes_dem"/$"total_votes").collect.map(s=>s.getDouble(0))

  val cg = ColorGradient((0.40, RedARGB), (0.60, BlueARGB))
  val sPlot = Plot.scatterPlot(lon,lat,"2016 Election Results","Longitude","Latitude",3,electRes.map(cg))
  //FXRenderer(sPlot,1280,720)

  println("")
  println("Between Class:")
  println("")
  println("1.)")

  val statesRate = statesData.filter(substring($"series_id",19,2) === "03")

  val statesRate1990 = statesRate.filter('year === 1990 && 'period === "M06")
  val statesRate1991 = statesRate.filter('year === 1991 && 'period === "M03")
  val statesRateFeb2001 = statesRate.filter('year === 2001 && 'period === "M02")
  val statesRateNov2001 = statesRate.filter('year === 2001 && 'period === "M11")
  val statesRate2007 = statesRate.filter('year === 2007 && 'period === "M11")
  val statesRate2009 = statesRate.filter('year === 2009 && 'period === "M07")

  val metroAreas = areaData.filter($"area_type_code" === "B")
  val microAreas = areaData.filter($"area_type_code" === "D")
  val countyAreas = areaData.filter($"area_type_code" === "F")
  
  val bin = (0.0 to 50.0 by 1.0).toArray
  
  //Histograms for a)
  
  val joinedMetroRate1990 = statesRate1990.join(metroAreas, $"series_id".contains($"area_code"))
  val joinedMetroRate1991 = statesRate1991.join(metroAreas, $"series_id".contains($"area_code"))
  //joinedMetroRate1990.printSchema()
  
  //metro 6/1990
  val histMetro1990 = joinedMetroRate1990.filter('value.isNotNull).select('value).as[Double].rdd.histogram(bin, true)
//  val plotMetro1990 = Plot.histogramPlot(bin, hist1990, BlueARGB, false, "Metro 1990", "Value", "Count")
  //FXRenderer(plot1990, 500, 300)
  
  //metro 3/1991
  val histMetro1991 = joinedMetroRate1991.filter('value.isNotNull).select('value).as[Double].rdd.histogram(bin, true)
//  val plot1991 = Plot.histogramPlot(bin, hist1991, BlueARGB, false, "Metro 1991", "Value", "Count")
  //FXRenderer(plot1991, 500, 300)
  
  //Metros 2001
  val joinedMetroRateFeb2001 = statesRateFeb2001.join(metroAreas, $"series_id".contains($"area_code"))
  val joinedMetroRateNov2001 = statesRateNov2001.join(metroAreas, $"series_id".contains($"area_code"))
  
  val histMetroFeb2001 = joinedMetroRateFeb2001.filter('value.isNotNull).select('value).as[Double].rdd.histogram(bin, true)
  val histMetroNov2001 = joinedMetroRateNov2001.filter('value.isNotNull).select('value).as[Double].rdd.histogram(bin, true)
  
  //Metro 2007 & 09
  val joinedMetroRate2007 = statesRate2007.join(metroAreas, $"series_id".contains($"area_code"))
  val joinedMetroRate2009 = statesRate2009.join(metroAreas, $"series_id".contains($"area_code"))

  val histMetro2007 = joinedMetroRate2007.filter('value.isNotNull).select('value).as[Double].rdd.histogram(bin, true)
  val histMetro2009 = joinedMetroRate2009.filter('value.isNotNull).select('value).as[Double].rdd.histogram(bin, true)
  
  //SwiftVis if it is Int it is color
  //Metro historgramGrid
  val metroHistoGrid = Plot.histogramGrid(bin, Seq(Seq((histMetro1990,BlueARGB),(histMetro1991,BlueARGB)),Seq((histMetroFeb2001,BlackARGB),(histMetroNov2001,BlackARGB)),Seq((histMetro2007,RedARGB),(histMetro2009,RedARGB))), false, true, "Metropolitan HistogramGrid", "Value", "Count")
  FXRenderer(metroHistoGrid, 800, 600)
  
  
  val joinedMicroRate1990 = statesRate1990.join(microAreas, $"series_id".contains($"area_code"))
  val joinedMicroRate1991 = statesRate1991.join(microAreas, $"series_id".contains($"area_code"))
  //joinedMicroRate1990.printSchema()
  
  //micro 6/1990
  val histMicro1990 = joinedMicroRate1990.filter('value.isNotNull).select('value).as[Double].rdd.histogram(bin, true)

  //micro 3/1991
  val histMicro1991 = joinedMicroRate1991.filter('value.isNotNull).select('value).as[Double].rdd.histogram(bin, true)
  
  //Micro 2001
  val joinedMicroRateFeb2001 = statesRateFeb2001.join(microAreas, $"series_id".contains($"area_code"))
  val joinedMicroRateNov2001 = statesRateNov2001.join(microAreas, $"series_id".contains($"area_code"))
  
  val histMicroFeb2001 = joinedMicroRateFeb2001.filter('value.isNotNull).select('value).as[Double].rdd.histogram(bin, true)
  val histMicroNov2001 = joinedMicroRateNov2001.filter('value.isNotNull).select('value).as[Double].rdd.histogram(bin, true)
  
  //Micro 2007 & 09
  val joinedMicroRate2007 = statesRate2007.join(microAreas, $"series_id".contains($"area_code"))
  val joinedMicroRate2009 = statesRate2009.join(microAreas, $"series_id".contains($"area_code"))

  val histMicro2007 = joinedMicroRate2007.filter('value.isNotNull).select('value).as[Double].rdd.histogram(bin, true)
  val histMicro2009 = joinedMicroRate2009.filter('value.isNotNull).select('value).as[Double].rdd.histogram(bin, true)
  
  //SwiftVis if it is Int it is color
  //Micro historgramGrid
  val microHistoGrid = Plot.histogramGrid(bin, Seq(Seq((histMicro1990,BlueARGB),(histMicro1991,BlueARGB)),Seq((histMicroFeb2001,BlackARGB),(histMicroNov2001,BlackARGB)),Seq((histMicro2007,RedARGB),(histMicro2009,RedARGB))), false, true, "Micropolitan HistogramGrid", "Value", "Count")
  FXRenderer(microHistoGrid, 800, 600)
  
  val joinedcountyRate1990 = statesRate1990.join(countyAreas, $"series_id".contains($"area_code"))
  val joinedcountyRate1991 = statesRate1991.join(countyAreas, $"series_id".contains($"area_code"))
  //joinedcountyRate1990.printSchema()
  
  //county 6/1990
  val histcounty1990 = joinedcountyRate1990.filter('value.isNotNull).select('value).as[Double].rdd.histogram(bin, true)

  //county 3/1991
  val histcounty1991 = joinedcountyRate1991.filter('value.isNotNull).select('value).as[Double].rdd.histogram(bin, true)
  
  //county 2001
  val joinedcountyRateFeb2001 = statesRateFeb2001.join(countyAreas, $"series_id".contains($"area_code"))
  val joinedcountyRateNov2001 = statesRateNov2001.join(countyAreas, $"series_id".contains($"area_code"))
  
  val histcountyFeb2001 = joinedcountyRateFeb2001.filter('value.isNotNull).select('value).as[Double].rdd.histogram(bin, true)
  val histcountyNov2001 = joinedcountyRateNov2001.filter('value.isNotNull).select('value).as[Double].rdd.histogram(bin, true)
  
  //county 2007 & 09
  val joinedcountyRate2007 = statesRate2007.join(countyAreas, $"series_id".contains($"area_code"))
  val joinedcountyRate2009 = statesRate2009.join(countyAreas, $"series_id".contains($"area_code"))

  val histcounty2007 = joinedcountyRate2007.filter('value.isNotNull).select('value).as[Double].rdd.histogram(bin, true)
  val histcounty2009 = joinedcountyRate2009.filter('value.isNotNull).select('value).as[Double].rdd.histogram(bin, true)
  
  //SwiftVis if it is Int it is color
  //county historgramGrid
  val countyHistoGrid = Plot.histogramGrid(bin, Seq(Seq((histcounty1990,BlueARGB),(histcounty1991,BlueARGB)),Seq((histcountyFeb2001,BlackARGB),(histcountyNov2001,BlackARGB)),Seq((histcounty2007,RedARGB),(histcounty2009,RedARGB))), false, false, "County Histogram Grids", "Value", "Count")
  FXRenderer(countyHistoGrid, 800, 600)
  
  println("Out of Class #2")

  val stateWithArea = statesRate.filter($"year"==="2016" && $"period"==="M11").join(areaData.filter($"area_type_code" === "F"), $"series_id".contains($"area_code"))
  val totalJoin = stateWithArea.join(electResults,$"area_text".contains(concat($"county_name",lit(", "),$"state_abbr"))).orderBy('total_votes.desc) //.select('value, 'per_dem)//.collect.toSeq
  
  val correlation = totalJoin.agg(corr($"value",$"per_dem"))
  correlation.show(false)
  
  //totalJoin.show(false)
  
  val population = totalJoin.filter('total_votes.isNotNull).select('total_votes).collect().map(a => a.getDouble(0))
  val unemployment = totalJoin.filter('value.isNotNull).select('value).collect().map(a => a.getDouble(0))
  val partyVote = totalJoin.filter('votes_dem.isNotNull && 'votes_gop.isNotNull).select('votes_dem / 'votes_gop).collect.map(a => a.getDouble(0))
  
  val cg3 = ColorGradient((0.5,RedARGB),(2.0, BlueARGB))
  val scatter = Plot.scatterPlot(population, unemployment, "Voting Tendency for Unemployed","Total Votes" , "Unemployment", population.map(0.02*math.sqrt(_)), partyVote.map(cg3))
  FXRenderer(scatter)
  
  println("Out of Class Q#3")
  
  val voterTurnout = totalJoin.filter('value.isNotNull && 'total_votes.isNotNull).select('total_votes / 'value).collect().map(a => a.getDouble(0))

  val scatterQ3 = Plot.scatterPlot(voterTurnout, unemployment, "Voter Turnout for Unemployed","Voter Turnout" , "Unemployment", voterTurnout.map(0.03*math.sqrt(_)), partyVote.map(cg3))
  FXRenderer(scatterQ3)
  
  println("\n")
  spark.stop()
}