package sparkrdd

import org.apache.spark.sql.SparkSession
case class Area(code:String,text:String)
case class Series(id:String, area:String,measure :String ,title:String)
case class LAData(id:String,year:Int,period:Int,value: Double)

object RDDJoin {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Joins").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val areas=spark
        .sparkContext
        .textFile("data/la.area.txt").filter(!_.contains("area_type"))
        .map{
          line => {
            val p = line.split("\t").map(_.trim)
            Area(p(1),p(2))
          }
        }.cache()


    val series=spark
        .sparkContext
        .textFile("data/la.series.txt")
        .filter(!_.contains("area_code"))
        .map{
          line =>
            {
               val p=line.split("\t").map(_.trim)
              Series(p(0),p(2),p(3),p(6))
            }
        }.cache()

    val data=spark
        .sparkContext
        .textFile("data/la.data.30.Minnesota.txt")
        .filter(!_.contains("year"))
        .map{
          line => {
            val p = line.split("\t").map(_.trim)
            LAData(p(0),p(1).toInt,p(2).drop(1).toInt,p(3).toDouble)
          }
        }.cache()


    spark.stop()


  }

}
