package practice

import org.apache.spark.sql.SparkSession
import scalafx.application.JFXApp
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._

case class TempData2(day: Int, doy: Int, month: Int, state: String,
  year: Int, precip: Double, tave: Double, tmax: Double, tmin: Double)

object TempDataset extends JFXApp {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  val data = spark.read.schema(Encoders.product[TempData2].schema).
    option("header", true).
    csv("/users/mlewis/CSCI3395-F17/InClass/SanAntonioTemps.csv").
    as[TempData2]
    
  data.show
  
  val hotDays = data.filter(_.tmax > 100) // or ('tmax > 100)
  hotDays.show(false)

  val myFunc = udf((s:String) => s.filter(_ != '\'').toInt)  //spark.sql requires .register in order to us myFunc
    data.select(myFunc('state)).show()
    
  //groupby & aggregate! User aggregation
  // Untype works with datasetrows[Untype] 
  
  //Weighted Average using aggregator 
    //Buffer = finish & we want it to be tuple (sum, total weight)
    // everything n = TempData2
    //everything out = Double
    //Buff = Double
    //temperatures weighted by precipitation
    //
  
  spark.stop()
}