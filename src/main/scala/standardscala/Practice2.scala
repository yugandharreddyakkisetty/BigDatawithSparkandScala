package standardscala

import standardscala.TempData.toDoubleorNeg

case class  TempData1(day:Int,doy:Int,month:Int,year:Int,precip:Double,snow:Double,tav:Double,tmax:Double,tmin:Double)

object Practice2 {
  def main(args: Array[String]): Unit = {
    val source = scala.io.Source.fromFile("MN212142_9392.csv")
    val lines = source.getLines().drop(1)

    val data=lines.flatMap {
      line => {
        val p=line.split(",")
        if(p(7)=="." || p(8) == "." || p(9) == ".") Seq.empty
        else
          Seq(
            TempData1(
              p(0).toInt,p(1).toInt,p(2).toInt,p(4).toInt,
              toDoubleorNeg(p(5)),toDoubleorNeg(p(6)),p(7).toDouble,p(8).toDouble,p(9).toDouble
            )
          )
      }
    }.toList


    // Finding hotest days
    // method 1 : following logic needs two iterations of data

    val maxTemp = data.map(t=>t.tmax).max
    val hotDays= data.filter(t=>t.tmax == maxTemp)
    println(s"Hot Days are(method 1) $hotDays")
    // method 2: following logic takes only iteration of data
    val hotDay1 = data.maxBy(t=>t.tmax)
    println(s"Hot Days are(method 2) $hotDay1")
    // method 3:
    val hotDay2 = data.reduceLeft((d1,d2)=> if (d1.tmax >= d2.tmax) d1 else d2)
    println(s"Hot Days are(method 3) $hotDay2")

    // count of rainy days
    val rainyDaysCount = data.count(t=>t.precip >= 1.0)
    println(s"Rainy Days $rainyDaysCount and ${rainyDaysCount*100.0/data.length} %")

    // average temperature of rainy days
    // method 1

    val (rainSum,rainyCount2) = data.foldLeft((0.0,0)){ case ((sum,cnt),td) => {
      if(td.precip < 1.0) (sum,cnt) else (sum+td.tmax,cnt+1)
    }}
    println(s"Rainy Days $rainyCount2 and Average is  ${rainSum/rainyCount2} %")

    // method 2
    val rainyTemps = data.flatMap(td=>if(td.precip<1.0 ) Seq.empty else Seq(td.tmax))
    println(s"Rainy Days  Average Temprature is  ${rainyTemps.sum/rainyTemps.length} %")

    // method 3

    val (rainySum3,rainyCoun3) = data.aggregate((0.0,0))(
      {case ((s1,c1),td) => if(td.precip < 1.0) (s1,c1) else (s1+td.tmax,c1+1)},
      {case ((s1,c1),(s2,c2)) => (s1+s2,c1+c2)}
    )

    println(s"Rainy Days  Average Temprature is  ${rainySum3/rainyCoun3} %")

    val monthlyGroups = data.groupBy(td => td.month)
    val monthlyTemps = monthlyGroups.map{
      case (m,days) => {
        m -> days.foldLeft(0.0){case (s,td) => s+td.tmax} / days.length
      }
    }




    monthlyTemps.toSeq.sortBy(_._1) foreach println
    monthlyTemps.toSeq.sortWith(_._1 < _._1) foreach println








    source.close()


  }
}
