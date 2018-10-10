package sparkrdd

import org.apache.spark.sql.SparkSession
import standardscala.TempData
import standardscala.TempData.toDoubleorNeg

object RDDTempData {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .master("local")
      .appName("Bigdata with RDD")
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

    // hottest day

    // date of highest temp
    val maxTemp=data.map(_.tmax).max
    val hotDays=data.filter(_.tmax == maxTemp)
    println(s"Hottest days are ${hotDays.collect().mkString(",")}")

    println(data.max()(Ordering.by(_.tmax)))
    println(data.reduce((td1,td2) => if(td1.tmax >= td2.tmax ) td1 else td2))





  } // main -- RDDTempData

} // object -- RDDTempData
