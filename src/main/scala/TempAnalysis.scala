
case class TempData(day:Int, doy:Int, month:Int, year:Int, precip: Double, taverage:Double, tmax:Double, tmin:Double)

object TempAnalysis {
  def main(args:Array[String]):Unit = {

    val source = io.Source.fromFile("/users/mlewis/CSCI1320-S17/SanAntonioTemps.csv")

    val lines = source.getLines.drop(2)

    //working with big data Array, tuples or case classes!
    val tempData = lines.map { line =>
    // val p = line.split(",").filter(!_.r\'').map(_.toDouble) //only works for doubles
    val p = line.split(",")
    TempData(p(0).toInt, p(1).toInt,p(2).toInt, p(4).toInt, p(5).toDouble, p(6).toDouble, p(7).toDouble, p(8).toDouble)
}.toArray

    //close to a while loop
    val (tempSum, tempCnt) = tempData.foldLeft(0.0 -> 0.0) { case ((sum, cnt), td) =>
      if(td.precip >= 1) (sum+td.tmax, cnt+1) else (sum, cnt)
    }
    //println("Temp avg Data "+tempSum/tempCnt)

    val byMonth:Map[Int, Array[TempData]] = tempData.groupBy(_.month)
    val monthlyAveTemp = byMonth.map { case (month, tds) =>
      val aveTemp = tds.foldLeft(0.0) { case (sum, td) => sum+td.tmax } / tds.length
	(month, aveTemp)
    }
    //monthlyAveTemp.toSeq.sortBy(_._1) foreach println

    val monthlyAvePrecip = byMonth.map { case (month, tds) =>
      val avePrecip = tds.foldLeft(0.0) { case (sum, td) => sum+td.precip } / tds.length
	(month, avePrecip)
    }
    //monthlyAvePrecip.toSeq.sortBy(_._1) foreach println

    val monthlyMedianPrecip = byMonth.map { case (month, tds) =>
	val sortedPrecip = tds.sortBy(_.precip)
	(month, sortedPrecip(tds.length/2))
    }
    monthlyMedianPrecip.toSeq.sortBy(_._1) foreach println

    //println(tempData.reduceLeft((a,b) => if(a.tmax >b.tmax) a else b))
    //println(tempData.foldLeft(tempData(0))((a,b) => if(a.tmax >b.tmax) a else b))
    //println("max temp: "+tempData.maxBy(_.tmax))

    //println("max by precip " +tempData.maxBy(_.precip))

    //wrong goes through collection 2
    //val rainTemps = tempData.filter(_.precip >= 1).map(_.tmax)

    val rainTemps = tempData.flatMap(td => if(td.precip >= 1) Seq(td.tmax) else Seq.empty)
    //rainTemps.take(5) foreach println

    source.close
  }
}
