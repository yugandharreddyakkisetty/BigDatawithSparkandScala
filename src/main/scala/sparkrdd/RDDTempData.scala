package sparkrdd

import org.apache.spark.sql.SparkSession
import standardscala.TempData
import standardscala.TempData.toDoubleorNeg

object RDDTempData {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .master("local")
      .appName("BigData with RDD")
      .getOrCreate()

    spark.sparkContext.setLogLevel("Error")
    val lines=spark.sparkContext.textFile("MN212142_9392.csv").filter(!_.contains("Day"))
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

    // hottest day
    // date of highest temp

    // method 1
    val maxTemp=data.map(_.tmax).max
    val hotDays=data.filter(_.tmax == maxTemp)
    println(s"Hottest days are ${hotDays.collect().mkString(",")}")

    // method 2
    println(data.max()(Ordering.by(_.tmax)))

    // method 3
    println(data.reduce((td1,td2) => if(td1.tmax >= td2.tmax ) td1 else td2))

    // counting rainy days
    val rainyCount = data.filter(_.precip >= 1.0).count()
    println(s"There are $rainyCount rainy days. Percentage : ${rainyCount*100.0/data.count}")

    // Average max temperature on rainy days

    // method 1
    val rainyDays= data.filter(_.precip >=1.0).map(_.tmax)
    val rainySum1= rainyDays.sum()
    val rainyCount1=rainyDays.count()
    println(s"Average max temperature on rainy days(using filter) ${rainySum1/rainyCount1}")

    // method 2
    val (rainySum2,rainyCount2)=data.aggregate((0.0 -> 0))(
      {case ((s,c),td) => if(td.precip < 1.0) (s,c) else (s+td.tmax,c+1)},
      {case ((s1,c1),(s2,c2)) => (s1+s2,c1+c2)}
    )
    println(s"Average max temperature on rainy days(using Aggregate) ${rainySum2/rainyCount2}")

    // method 3
    val rainyDays1 = data.flatMap(td => if(td.precip < 1.0 ) Seq.empty else Seq(td.tmax))
    println(s"Average max temperature on rainy days(using flattMap) ${rainyDays1.sum/rainyDays1.count}")

    // groupby on months

    val monthsGroup = data.groupBy(_.month)
    val monthlyTemp=monthsGroup.map{case(m,days) =>
      (m,days.foldLeft(0.0){case (sum,td) => sum+td.tmax}/days.size)
    }

      monthlyTemp.collect.sortBy(_._1).foreach(println)


    /*
    *
    *
    * PairRDD
    *
    *
      */

      // Average max temp by year

    val keyedByYear=data.map(td=>td.year -> td)
    val averageTempByYear = keyedByYear.aggregateByKey(0.0 -> 0)(
      {case ((sum,cnt),td) => (sum+td.tmax,cnt+1)},
      {case ((s1,c1),(s2,c2)) => (s1+s2,c1+c2)}
    ).mapValues( x =>x._1 / x._2)

    averageTempByYear.collect().sortBy(_._1).foreach(println)







  } // main -- RDDTempData
} // object -- RDDTempData
