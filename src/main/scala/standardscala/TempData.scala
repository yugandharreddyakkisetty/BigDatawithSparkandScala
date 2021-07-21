package standardscala



case class TempData (
                    day:Int,
                    doy:Int,
                    month : Int,
                    year : Int,
                    precip : Double ,
                    snow : Double,
                    tave:Double,
                    tmax : Double,
                    tmin : Double
                    )
// TempData - temperature  data
// input file has dots for missing values




object TempData {

  def toDoubleorNeg(s:String):Double = {
    try {
      s.toDouble
    }
    catch {
      case _:NumberFormatException => -1
    }
  }

  def main(args: Array[String]): Unit = {
    val source = scala.io.Source.fromFile("MN212142_9392.csv")
    // drop header
    val lines = source.getLines().drop(1)


    /*
    // converting each line into TempData object
    // filter a record if it contain missing value at any location
    val data=lines.filterNot(_.contains(",.,")).map{ line =>
      val p = line.split(",")
      TempData(
        p(0).toInt,p(1).toInt,p(2).toInt,p(4).toInt,p(5).toDouble,
        p(6).toDouble,p(7).toDouble,p(8).toDouble,p(9).toDouble
      )
    }.toArray
*/



    /*
     1. Filter out records with missing values at some positions (7,8 and 9)
     2. Replace missing values with default value ( -1 ) at some positions (5 and 6)
    */

  val data=lines.flatMap {
    line => {
      val p=line.split(",")
      if(p(7)=="." || p(8) == "." || p(9) == ".") Seq.empty
      else
        Seq(
          TempData(
            p(0).toInt,p(1).toInt,p(2).toInt,p(4).toInt,
            toDoubleorNeg(p(5)),toDoubleorNeg(p(6)),p(7).toDouble,p(8).toDouble,p(9).toDouble
          )
        )
    }
  }.toList
    //.toList

    def ff()=data


    source.close()

    // date of highest temp
    val maxTemp=data.map(_.tmax).max
    val hotDays=data.filter(_.tmax == maxTemp)
    println(s"Hottest days are $hotDays")

    // above method of finding hottest days take two iterations through the dataset
    // following takes one iteration
    val hotDay1 = data.maxBy(_.tmax)

    println(s"Hottest day 1 is $hotDay1")

    val hotDay2= data.reduceLeft((d1,d2) => if(d1.tmax >= d2.tmax) d1 else d2)
    println(s"Hottest day2 is $hotDay2")

    // counting rainy days

    val rainyCount = data.count(_.precip >= 1.0)
    println(s"There are $rainyCount rainy days. Percentage : ${rainyCount*100.0/data.length}")


    // Average max temperature in rainy days

    val rainyDays=data.filter(_.precip >= 1.0)
    val rainySum1=rainyDays.map(_.tmax).sum
    println(s"Average max temperature on rainy days (using two passes) ${rainySum1/rainyCount}")


    val (rainySum2,rainyCount2) = data.foldLeft((0.0,0)){
      case ((sum,cnt),td) => if(td.precip < 1.0) (sum,cnt) else (sum+td.tmax,cnt+1)
    }



    println(s"Average max temperature on rainy days (using foldLeft): ${rainySum2/rainyCount2}")

    val rainyTemps=data.flatMap(td => if(td.precip < 1.0) Seq.empty else Seq(td.tmax))
    println(s"Average max temperature on rainy days (using flatMap)) ${rainyTemps.sum/rainyTemps.length}")

    val (rainySum3,rainyCount3)= data.aggregate((0.0,0))(
      {case ((s1,c1),td) => if(td.precip < 1.0) (s1,c1) else (s1+td.tmax,c1+1) },
      {case ((s1,c1),(s2,c2)) => (s1+s2,c1+c2)}
    )

    println(s"Average max temperature on rainy days (using aggregate): ${rainySum3/rainyCount3}")




    // Average temperature of each month

    val monthGroup=data.groupBy(_.month)

    val monthlyTemp=monthGroup.map{case(m,days) =>
      m -> days.foldLeft(0.0)((sum,td) => sum+td.tmax)/days.length
    }

    println(monthlyTemp)

    monthlyTemp.toSeq.sortBy(_._1) foreach println

  }
}
