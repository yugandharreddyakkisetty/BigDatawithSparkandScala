package sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql._
case class Series(sid:String, area:String,measure :String ,title:String)
case class LAData(id:String,year:Int,period:String,value: Double)
case class ZipData(zipCode:String,lat:Double,lon:Double,city:String,state:String,county:String)
case class ZipCountyData(lat:Double,lon:Double,state:String,county:String)
object TDatasets {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("Typed Datasets").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    val minneesota = spark.read
        .schema(Encoders.product[LAData].schema)
      .format("csv")
      .option("header",true)
      .option("sep","\t")
      .load("data/la.data.30.Minnesota.txt")
      .withColumn("id",trim('id))
        .as[LAData]
    val series = spark.read.textFile("data/la.series.txt") .map{
      line =>
      {
        val p=line.split("\t").map(_.trim)
        Series(p(0),p(2),p(3),p(6))
      }
    }

    val zipData = spark.read
        .schema(Encoders.product[ZipData].schema)
      .format("csv")
      .option("header",true)
      .option("delimiter",",")
      .load("data/zip_codes_states.csv").as[ZipData].filter(col("lat").isNotNull).cache()

    val location = zipData
      .groupByKey(z=>z.county -> z.state)
      .agg(avg('lat).as[Double],avg('lon).as[Double])
      .map{case ((county,state),lat,lon) => ZipCountyData(lat,lon,state,county)}


    val joined = minneesota.joinWith(series,'id === 'sid)
    val fullyJoined = joined.joinWith(location,col("_2")("title").contains(col("county")))
    fullyJoined.show()
    spark.close()
  }

}
