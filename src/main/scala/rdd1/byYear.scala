package rdd1

import scalafx.application.JFXApp
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

case class TempData(id: String, doy: Int, elem: String, dv: Double)
case class StationsData(id: String, latitude: Double, longitude: Double, elevation: Double, state: String,name:String)

//case class StationsData(id: String, latitude: Double, longitude: Double, elevation: Double, state: String, name: String, gsn: String, hcn: String, wmoID: Int)

object byYear extends JFXApp {
  val conf = new SparkConf().setAppName("Sample Application").setMaster("local[*]")
  val sc = new SparkContext(conf)
 
  val lines2017 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/2017.csv")
  val countries = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/ghcnd-countries.txt")
  val stations = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/ghcnd-stations.txt")

  //Q1 In Class
  
  val stationsInfo = stations.map { line =>
    val p = line //(" ") //need to split by number of characters 
    StationsData(p.substring(0,11), 
                 if(!p.substring(12,21).trim.isEmpty) p.substring(12,21).trim.toDouble else 0.0 ,
                 if(!p.substring(21,31).trim.isEmpty) p.substring(21,31).trim.toDouble else 0.0 ,
                 if(!p.substring(31,38).trim.isEmpty) p.substring(31,38).trim.toDouble else 0.0 ,
                 if(!p.substring(38,41).trim.isEmpty) p.substring(38,41).trim else "",
                 p.substring(41,72).trim())
  }
  
  
  val stationsTX = stationsInfo.filter(td => td.state=="TX")
  val stationCount = stationsTX.count()
  println("Number of Texas Stations " + stationCount) 
  
  //Q2 In Class
  
   val tempData = lines2017.map { line =>
    val p = line.split(",")
    TempData(p(0), p(1).toInt, p(2),p(3).toDouble)
  }
  
  val stationsTXWithData = stationsTX.map(td => td.id.distinct)
  val countTXStationData = stationsTXWithData.count()
  println("Texas Stations with Data " + countTXStationData)

  //how to get all the data of the stations in TX
  val seqStationIDTX = stationsTX.map(_.id).collect().toSeq
  val dataID2017 = tempData.filter(td => seqStationIDTX.contains(td.id) && (td.elem=="TMIN" || td.elem =="TMAX"))
  //intersection
  
  //Q3 In Class
  val temps2017 = tempData.filter(td => td.elem =="TMAX")
  val maxTemp2017 = temps2017.collect().maxBy(_.dv)
  println("Max temperature 2017 is " + maxTemp2017.dv + " on " +maxTemp2017.doy)
  
  val maxTempID2017 = maxTemp2017.id
  
  val maxTempLoc = stationsInfo.filter(td => td.id == maxTemp2017.id).map(_.name)
  println("Max Temp Location is below ")
//  maxTempLoc foreach println
  
  //Q4 In Class
  val stationIDs = stationsInfo.map(_.id)
  val seq2017IDs = tempData.map(td => td.id.distinct).collect().toSeq
  
  val stationsNoData = stationIDs.filter(td => seq2017IDs.contains(td)) 
  stationsNoData.take(5) foreach println
  val countStationsNoData = stationsNoData.count()
  
  println("huh")
  println("There are " +countStationsNoData+ " stations with no data")
  
  val stationsInTX = tempData.map(td => td.id.substring(0,2)=="TX"-> td)
  stationsInTX.take(5) foreach println

  
  //Q1: Grab ids in TX from stations data; compare array with 2017 data and get all rows with same IDs & with 4 char ELEM == "PRCP"; then find max dvalue 

  //to print Array of Stations Case Classes in TX
  
  //Q2: Same as Q1 but grab ids with first 2 char as "IN"; same for rest o steps
  
 
  //Q3: From Stations Data count number of stations with state =="TX" && name "San Antonio*"	
  
  
  
  //Q4: List of ids from Q3 compare with IDs in stationsData. Count IDs in stationData with elem = "TMAX" 

  //Q5: From answers in Q4; first id tmax divide by second tmax to get first daily increase in high temp. Then for following data check if the succeeding daily increase of tmax is greater then already recorded increase

  //Q6:  

}
