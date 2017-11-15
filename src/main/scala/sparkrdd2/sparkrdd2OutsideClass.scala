package sparkrdd2

import scalafx.application.JFXApp
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark._
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.FXRenderer 
import org.apache.spark.rdd.RDD

case class TempData3(id: String, doy: Int, elem: String, dv: Double)
case class StationsData(id: String, latitude: Double, longitude: Double, elevation: Double, state: String)

object sparkrdd2OutsideClass extends JFXApp {
  val conf = new SparkConf().setAppName("sparkrdd2OutsideClass").setMaster("spark://pandora00:7077")//spark://pandora00:7077")
  val sc = new SparkContext(conf)

  sc.setLogLevel("WARN")

  val w2017 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/2017.csv")
 // println(w2017.count)

  val stations = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/ghcnd-stations.txt").
    filter(line => !line.contains("Source") && !line.contains("Day"))
//  stations.take(5) foreach println 
 
  // 2017.csv Data
  val tempData = w2017.map { line =>
    val p = line.split(",")
    TempData3(p(0), p(1).toInt, p(2),p(3).toDouble)
  }
  //val dailyData = tempData.map(a => a.id)
  //dailyData.take(5) foreach println 

 //to print Array of Station Latitudes
  val stationsInfo = stations.map { line =>
    val p = line //(" ") //need to split by number of characters 
    StationsData(p.substring(0,12), 
                 if(!p.substring(12,21).trim.isEmpty) p.substring(12,21).trim.toDouble else 0.0 ,
                 if(!p.substring(21,31).trim.isEmpty) p.substring(21,31).trim.toDouble else 0.0 ,
                 if(!p.substring(31,38).trim.isEmpty) p.substring(31,38).trim.toDouble else 0.0 ,
                 p.substring(38,41))
  }
                 
                 /*, p.substring(72,76), p.substring(76,80),
                 if(!p.substring(80,85).trim.isEmpty) p.substring(80,85).trim.toInt else 0) 
  } */
  
  //3 Regions
  val stationsG1 = stationsInfo.filter(td => td.latitude < 35).map(a => a.id)
  val stationsG2 = stationsInfo.filter(td => td.latitude > 35 && td.latitude < 42).map(a => a.id)
  val stationsG3 = stationsInfo.filter(td => td.latitude > 43).map(a => a.id)

  //stationsG1.take(5) foreach println 
  
  
  //Question 2 out of class
  
  val listStations = stationsInfo.map(a => a.id-> (a.longitude,a.latitude))
 // listStations.take(5) foreach println
 
  val stationsWithTemp = tempData.filter(td => td.elem == "TMAX").map(a => a.id -> a.dv)
  //stationsWithTemp.take(5) foreach println
  
  val tempSumCount = stationsWithTemp.aggregateByKey((0.0, 0))({ case ((sum, cnt), td) =>
    (sum+td, cnt+1)
  }, { case ((s1, c1), (s2, c2)) => (s1+s2, c1+c2)})
 //   tempSumCount.take(5) foreach println
  val tempAverage = tempSumCount.mapValues(t => t._1 / t._2)
 // println("average temps")
 // tempAverage.take(5) foreach println
  
  //println("see")
  
  val joinFiles = listStations.join(stationsWithTemp)
 // joinFiles.take(5) foreach println
  
  val temps = joinFiles.collect().toList
 // println("Full Collection")
  //convert tenth of celcius to farenheit: (x/10)*32
  val latValue = -100 to 100 by 1
  val longs = -200 to 200 by 1
  val cg = ColorGradient((0.0,BlueARGB),((50.0/32)*10,GreenARGB), ((100.0/31)*10 ,RedARGB))

 // temps.take(5) foreach println
  println("check")
//  val tplot = Plot.scatterPlot(temps.map(_._2._1._1), temps.map(_._2._1._2), "Temps By Loc", "Longitude", "Latitude", 3.0, temps.map(a => cg(a._2._2)))
//  FXRenderer(tplot)
  
 /* def convertCelciusToFaren(tempList:RDD[StationsData]):RDD[StationsData] = {
    //tempList.take(5) foreach println
    println
  } */
   
  //Question 3 out of class
  
  def getAvgTempFromFile(year:Int) = {
    val path = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/"+year+".csv")
    val pathTempData = path.map { line =>
      val p = line.split(",")
      TempData3(p(0), p(1).toInt, p(2),p(3).toDouble)
    }
    val elem = pathTempData.filter(td => td.elem == "TMAX" || td.elem=="TMIN").map(a => a.id -> a)
    
    val stationSumCount = elem.aggregate((0.0, 0))((x,y) => (x._1 + y._2.dv, x._2+1), (x,y) => (x._1+y._1,x._2+y._2))
    
    val stationAverage = stationSumCount._1 / stationSumCount._2
    stationAverage
  }
  println(getAvgTempFromFile(1897))
  
  val rddAvgTemps = Array(getAvgTempFromFile(1897),
      getAvgTempFromFile(1898),
      getAvgTempFromFile(1899),
      getAvgTempFromFile(1900),
      getAvgTempFromFile(1901),
      getAvgTempFromFile(1902),
      getAvgTempFromFile(1903),
      getAvgTempFromFile(1904),
      getAvgTempFromFile(1905),
      getAvgTempFromFile(1906),
      getAvgTempFromFile(1907),
      getAvgTempFromFile(1908),
      getAvgTempFromFile(1909),
      getAvgTempFromFile(1910),
      getAvgTempFromFile(1911),
      getAvgTempFromFile(1912),
      getAvgTempFromFile(1913),
      getAvgTempFromFile(1914),
      getAvgTempFromFile(1915),
      getAvgTempFromFile(1916),
      getAvgTempFromFile(1917),
      getAvgTempFromFile(1918),
      getAvgTempFromFile(1919),
      getAvgTempFromFile(1920),
      getAvgTempFromFile(1921),
      getAvgTempFromFile(1922),
      getAvgTempFromFile(1923),
      getAvgTempFromFile(1924),
      getAvgTempFromFile(1925),
      getAvgTempFromFile(1926),
      getAvgTempFromFile(1927),
      getAvgTempFromFile(1928),
      getAvgTempFromFile(1929),
      getAvgTempFromFile(1930),
      getAvgTempFromFile(1931),
      getAvgTempFromFile(1932),
      getAvgTempFromFile(1933),
      getAvgTempFromFile(1934),
      getAvgTempFromFile(1935),
      getAvgTempFromFile(1936),
      getAvgTempFromFile(1937),
      getAvgTempFromFile(1938),
      getAvgTempFromFile(1939),
      getAvgTempFromFile(1940),
      getAvgTempFromFile(1941),
      getAvgTempFromFile(1942),
      getAvgTempFromFile(1943),
      getAvgTempFromFile(1944),
      getAvgTempFromFile(1945),
      getAvgTempFromFile(1946),
      getAvgTempFromFile(1947),
      getAvgTempFromFile(1948),
      getAvgTempFromFile(1949),
      getAvgTempFromFile(1950),
      getAvgTempFromFile(1951),
      getAvgTempFromFile(1952),
      getAvgTempFromFile(1953),
      getAvgTempFromFile(1954),
      getAvgTempFromFile(1955),
      getAvgTempFromFile(1956),
      getAvgTempFromFile(1957),
      getAvgTempFromFile(1958),
      getAvgTempFromFile(1959),
      getAvgTempFromFile(1960),
      getAvgTempFromFile(1961),
      getAvgTempFromFile(1962),
      getAvgTempFromFile(1963),
      getAvgTempFromFile(1964),
      getAvgTempFromFile(1965),
      getAvgTempFromFile(1966),
      getAvgTempFromFile(1967),
      getAvgTempFromFile(1968),
      getAvgTempFromFile(1969),
      getAvgTempFromFile(1970),
      getAvgTempFromFile(1971),
      getAvgTempFromFile(1972),
      getAvgTempFromFile(1973),
      getAvgTempFromFile(1974),
      getAvgTempFromFile(1975),
      getAvgTempFromFile(1976),
      getAvgTempFromFile(1977),
      getAvgTempFromFile(1978),
      getAvgTempFromFile(1979),
      getAvgTempFromFile(1980),
      getAvgTempFromFile(1981),
      getAvgTempFromFile(1982),
      getAvgTempFromFile(1983),
      getAvgTempFromFile(1984),
      getAvgTempFromFile(1985),
      getAvgTempFromFile(1986),
      getAvgTempFromFile(1987),
      getAvgTempFromFile(1988),
      getAvgTempFromFile(1989),
      getAvgTempFromFile(1990),
      getAvgTempFromFile(1991),
      getAvgTempFromFile(1992),
      getAvgTempFromFile(1993),
      getAvgTempFromFile(1994),
      getAvgTempFromFile(1995),
      getAvgTempFromFile(1996),
      getAvgTempFromFile(1997),
      getAvgTempFromFile(1998),
      getAvgTempFromFile(1999),
      getAvgTempFromFile(2000),
      getAvgTempFromFile(2001),
      getAvgTempFromFile(2002),
      getAvgTempFromFile(2003),
      getAvgTempFromFile(2004),
      getAvgTempFromFile(2005),
      getAvgTempFromFile(2006),
      getAvgTempFromFile(2007),
      getAvgTempFromFile(2008),
      getAvgTempFromFile(2009),
      getAvgTempFromFile(2010),
      getAvgTempFromFile(2011),
      getAvgTempFromFile(2012),
      getAvgTempFromFile(2013),
      getAvgTempFromFile(2014),
      getAvgTempFromFile(2015),
      getAvgTempFromFile(2016),
      getAvgTempFromFile(2017) 
  )
  
//  val rddTemps = sc.parallelize(rddAvgTemps)
  //rddTemps.take(5) foreach println
  println("Problem 3 a")
  //avgTemp1897.collect().sortBy(_._1) foreach println
  
  println("Problem 3 c")
//  val bins = (0.0 to 500.0 by 1.0).toArray
//  val hist = rddTemps.histogram(bins, true)
//  val plot = Plot.histogramPlot(bins, hist, RedARGB, false)
//  FXRenderer(plot)
  
  def getStationAvgTempFromBothYears(year:Int) {
    val path = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/2017.csv")
    val pathTempData = path.map { line =>
      val p = line.split(",")
      TempData3(p(0), p(1).toInt, p(2),p(3).toDouble)
    }
    
    val elem = pathTempData.filter(td => td.elem == "TMAX" || td.elem=="TMIN").map(a => a.id -> a)
    val stationSumCount = elem.aggregate((0.0, 0))((x,y) => (x._1 + y._2.dv, x._2+1), (x,y) => (x._1+y._1,x._2+y._2))
    val stationAverage = stationSumCount._1 / stationSumCount._2
    
    val path2 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/1897.csv")
    val pathTempData2 = path2.map { line =>
      val p = line.split(",")
      TempData3(p(0), p(1).toInt, p(2),p(3).toDouble)
    }
    val elem2 = pathTempData2.filter(td => td.elem == "TMAX" || td.elem=="TMIN").map(a => a.id -> a)
    val stationSumCount2 = elem2.aggregate((0.0, 0))((x,y) => (x._1 + y._2.dv, x._2+1), (x,y) => (x._1+y._1,x._2+y._2))
    val stationAverage2 = stationSumCount2._1 / stationSumCount2._2
    
    val joinedStationsBothYears = (stationAverage2 + stationAverage )/2
    //joinedStationsBothYears
    
  }
  //val test = getStationAvgTempFromBothYears()
  println("Problem 3 b")
  //test.take(5) foreach println
  
  println(getStationAvgTempFromBothYears(1897))
  
    val rddAvgTemps3d = Array(getStationAvgTempFromBothYears(1897),
      getAvgTempFromFile(1898),
      getAvgTempFromFile(1899),
      getAvgTempFromFile(1900),
      getAvgTempFromFile(1901),
      getAvgTempFromFile(1902),
      getAvgTempFromFile(1903),
      getAvgTempFromFile(1904),
      getAvgTempFromFile(1905),
      getAvgTempFromFile(1906),
      getAvgTempFromFile(1907),
      getAvgTempFromFile(1908),
      getAvgTempFromFile(1909),
      getAvgTempFromFile(1910),
      getAvgTempFromFile(1911),
      getAvgTempFromFile(1912),
      getAvgTempFromFile(1913),
      getAvgTempFromFile(1914),
      getAvgTempFromFile(1915),
      getAvgTempFromFile(1916),
      getAvgTempFromFile(1917),
      getAvgTempFromFile(1918),
      getAvgTempFromFile(1919),
      getAvgTempFromFile(1920),
      getAvgTempFromFile(1921),
      getAvgTempFromFile(1922),
      getAvgTempFromFile(1923),
      getAvgTempFromFile(1924),
      getAvgTempFromFile(1925),
      getAvgTempFromFile(1926),
      getAvgTempFromFile(1927),
      getAvgTempFromFile(1928),
      getAvgTempFromFile(1929),
      getAvgTempFromFile(1930),
      getAvgTempFromFile(1931),
      getAvgTempFromFile(1932),
      getAvgTempFromFile(1933),
      getAvgTempFromFile(1934),
      getAvgTempFromFile(1935),
      getAvgTempFromFile(1936),
      getAvgTempFromFile(1937),
      getAvgTempFromFile(1938),
      getAvgTempFromFile(1939),
      getAvgTempFromFile(1940),
      getAvgTempFromFile(1941),
      getAvgTempFromFile(1942),
      getAvgTempFromFile(1943),
      getAvgTempFromFile(1944),
      getAvgTempFromFile(1945),
      getAvgTempFromFile(1946),
      getAvgTempFromFile(1947),
      getAvgTempFromFile(1948),
      getAvgTempFromFile(1949),
      getAvgTempFromFile(1950),
      getAvgTempFromFile(1951),
      getAvgTempFromFile(1952),
      getAvgTempFromFile(1953),
      getAvgTempFromFile(1954),
      getAvgTempFromFile(1955),
      getAvgTempFromFile(1956),
      getAvgTempFromFile(1957),
      getAvgTempFromFile(1958),
      getAvgTempFromFile(1959),
      getAvgTempFromFile(1960),
      getAvgTempFromFile(1961),
      getAvgTempFromFile(1962),
      getAvgTempFromFile(1963),
      getAvgTempFromFile(1964),
      getAvgTempFromFile(1965),
      getAvgTempFromFile(1966),
      getAvgTempFromFile(1967),
      getAvgTempFromFile(1968),
      getAvgTempFromFile(1969),
      getAvgTempFromFile(1970),
      getAvgTempFromFile(1971),
      getAvgTempFromFile(1972),
      getAvgTempFromFile(1973),
      getAvgTempFromFile(1974),
      getAvgTempFromFile(1975),
      getAvgTempFromFile(1976),
      getAvgTempFromFile(1977),
      getAvgTempFromFile(1978),
      getAvgTempFromFile(1979),
      getAvgTempFromFile(1980),
      getAvgTempFromFile(1981),
      getAvgTempFromFile(1982),
      getAvgTempFromFile(1983),
      getAvgTempFromFile(1984),
      getAvgTempFromFile(1985),
      getAvgTempFromFile(1986),
      getAvgTempFromFile(1987),
      getAvgTempFromFile(1988),
      getAvgTempFromFile(1989),
      getAvgTempFromFile(1990),
      getAvgTempFromFile(1991),
      getAvgTempFromFile(1992),
      getAvgTempFromFile(1993),
      getAvgTempFromFile(1994),
      getAvgTempFromFile(1995),
      getAvgTempFromFile(1996),
      getAvgTempFromFile(1997),
      getAvgTempFromFile(1998),
      getAvgTempFromFile(1999),
      getAvgTempFromFile(2000),
      getAvgTempFromFile(2001),
      getAvgTempFromFile(2002),
      getAvgTempFromFile(2003),
      getAvgTempFromFile(2004),
      getAvgTempFromFile(2005),
      getAvgTempFromFile(2006),
      getAvgTempFromFile(2007),
      getAvgTempFromFile(2008),
      getAvgTempFromFile(2009),
      getAvgTempFromFile(2010),
      getAvgTempFromFile(2011),
      getAvgTempFromFile(2012),
      getAvgTempFromFile(2013),
      getAvgTempFromFile(2014),
      getAvgTempFromFile(2015),
      getAvgTempFromFile(2016),
      getAvgTempFromFile(2017) 
  )
  
  
  //Question 4
  /* The huge flaw in problem 3 is that with the limitation of not using for loops we 
   * have to manually type in the contents of our Array/List for the average from the files
   * for each given year.
   */
  
  
  
  
  
//  val avgThroughYears = yearAverage.collect()
//  val tplot = Plot.scatterPlot(temps.map(_._1), temps.map(_._2), "Temps", "year", "Temp", 10, 0xff55aa99)
//  FXRenderer(tplot)
  
  
  //Question 1
  

//  val test = stationsG1.collect({case i:String => i})
//  test.take(5) foreach println  

//  println(test(1))

  /*
   * 
  //a
  val byYear = tempData.map(td => td.year -> td)
  val yearlySumCount = byYear.aggregateByKey((0.0, 0))({ case ((sum, cnt), td) =>
    (sum+td.tmax, cnt+1)
  }, { case ((s1, c1), (s2, c2)) => (s1+s2, c1+c2)})
  val yearAverage = yearlySumCount.mapValues(t => t._1 / t._2)
  yearAverage.collect().sortBy(_._1) foreach println
  
  
   

  val bins = (0.0 to 500.0 by 1.0).toArray
  val hist = highTemps.histogram(bins, true)
  val plot = Plot.histogramPlot(bins, hist, RedARGB, false)
  FXRenderer(plot)
  
  val temps = yearAverage.collect()
  val tplot = Plot.scatterPlot(temps.map(_._1), temps.map(_._2), "Temps", "year", "Temp", 10, 0xff55aa99)
  FXRenderer(tplot)
  
  sc.stop()
  */

  
  
}




