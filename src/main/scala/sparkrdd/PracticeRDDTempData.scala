package sparkrdd
import org.apache.spark.sql.SparkSession
import standardscala.TempData
import standardscala.TempData.toDoubleorNeg

object PracticeRDDTempData {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .master("local")
      .appName("BigData with RDD")
      .getOrCreate()
    spark.sparkContext.setLogLevel("Error")
    val lines = spark.sparkContext.textFile("MN212142_9392.csv").filter(!_.contains("Day"))
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
    }
    data.cache()

    // hotest days

    // method 1
    val maxTemp = data.map(_.tmax).max()
    val hotestDays = data.filter(_.tmax == maxTemp).take(1)
    println(s"Hotest days are ${hotestDays.mkString(",")}")
    //method 2
    val hotestDay1 = data.max()(Ordering.by(_.tmax))
    println(s"Hotest days are $hotestDay1 ")
    //method 3
    val hotestDay2 = data.reduce{case(d1,d2) => if(d1.tmax >= d2.tmax) d1 else d2}
    println(s"Hotest days are $hotestDay2 ")

    // count of rainy days
    val rainyDaysCount = data.filter(_.precip >= 1.0).count()
    println(s"Rainy Days $rainyDaysCount and ${rainyDaysCount*100.0/data.count} %")


    // average temperature of rainy days


    // method 2
    val rainyTemps = data.flatMap(td=>if(td.precip<1.0 ) Seq.empty else Seq(td.tmax))
    println(s"Rainy Days  Average Temprature is  ${rainyTemps.sum/rainyTemps.count} %")

    // method 3

    val (rainySum3,rainyCoun3) = data.aggregate((0.0,0))(
      {case ((s1,c1),td) => if(td.precip < 1.0) (s1,c1) else (s1+td.tmax,c1+1)},
      {case ((s1,c1),(s2,c2)) => (s1+s2,c1+c2)}
    )

    println(s"Rainy Days  Average Temprature is  ${rainySum3/rainyCoun3} %")
    val monthlyGroups = data.groupBy(td => td.month)
    val monthlyTemps = monthlyGroups.map{
      case (m,days) => {
        m -> days.foldLeft(0.0){case (s,td) => s+td.tmax} / days.size
      }
    }




    monthlyTemps.collect.sortBy(_._1) foreach println
    monthlyTemps.collect.sortWith(_._1 < _._1) foreach println
    // Pair RDDs

    val keyByYear = data.map(t=>t.year -> t)
    val averageTempByYear = keyByYear.aggregateByKey((0.0,0))(
      {case((s,c),t) => (s+t.tmax,c+1)},
      {case ((s1,c1),(s2,c2)) => (s1+s2,c1+c2)}
    )
    averageTempByYear.map{case (k,(s,c))=>(k,s/c)} foreach println


  }
}
