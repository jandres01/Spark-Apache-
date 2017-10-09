package practice

import scalafx.application.JFXApp
import org.apache.spark._
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.FXRenderer
import org.apache.spark.rdd.RDD

object HelloSpark extends JFXApp {
  val conf = new SparkConf().setAppName("Hello Spark").setMaster("local[*]")//spark://pandora00:7077")
  val sc = new SparkContext(conf)

  sc.setLogLevel("WARN")

	val w2017 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/2017.csv")
	println(w2017.count)

  val lines = sc.textFile("/users/mlewis/CSCI1320-S17/SanAntonioTemps.csv").
    filter(line => !line.contains("Source") && !line.contains("Day"))
  lines.take(5) foreach println

  val tempData = lines.map { line =>
    val p = line.split(",")
    TempData(p(0).toInt, p(1).toInt, p(2).toInt,
      p(4).toInt, p(5).toDouble, p(6).toDouble,
      p(7).toDouble, p(8).toDouble)
  }
  val highTemps = tempData.map(_.tmax)
  println(highTemps.stdev())
  
  val byYear = tempData.map(td => td.year -> td)
  val yearlySumCount = byYear.aggregateByKey((0.0, 0))({ case ((sum, cnt), td) =>
    (sum+td.tmax, cnt+1)
  }, { case ((s1, c1), (s2, c2)) => (s1+s2, c1+c2)})
  val yearAverage = yearlySumCount.mapValues(t => t._1 / t._2)
  yearAverage.collect().sortBy(_._1) foreach println
  
  val bins = (0.0 to 120.0 by 1.0).toArray
  val hist = highTemps.histogram(bins, true)
  val plot = Plot.histogramPlot(bins, hist, RedARGB, false)
  FXRenderer(plot)
  
  val temps = yearAverage.collect()
  val tplot = Plot.scatterPlot(temps.map(_._1), temps.map(_._2), "Temps", "year", "Temp", 10, 0xff55aa99)
  FXRenderer(tplot)
  
  sc.stop()
}
