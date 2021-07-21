package sparksql
import java.sql.Date

import org.apache.avro.generic.GenericData
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Encoders._
case class TData(sid:String,date:Date,mtype:String,value:Double)

object NOAAData {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Bigdata with Spark").master("local[*]").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    val tShcema = StructType(Array(
      StructField("sid",StringType,false),
      StructField("date",DateType,true),
      StructField("mtype",StringType,true),
      StructField("value",DoubleType,true)
    ))
    val sSchema = StructType(Array(
      StructField("sid",StringType,false),
      StructField("lat",DoubleType,false),
      StructField("lon",DoubleType,false),
      StructField("name",StringType,false)

    ))
/*
      using encoders and case class to inferSchema
      val data2020 = spark
      .read.schema(Encoders.product[TData].schema)
      .format("csv")
      .option("sep",",")
      .option("dateFormat","yyyyMMdd")
      .load("data/2020_6.csv")
    data2020.show()*/

    // using StructType to inferSchema
    val data2020 = spark
      .read.schema(tShcema)
      .format("csv")
      .option("sep",",")
      .option("dateFormat","yyyyMMdd")
      .load("data/2020_6.csv")
    data2020.show()

    val stationsRDD = spark.sparkContext.textFile("data/ghcnd-stations.txt").map(line=> {
      val id = line.substring(0, 11)
      val lat = line.substring(12, 20).toDouble
      val lon = line.substring(21, 30).toDouble
      val name = line.substring(41, 71)
      Row(id, lat, lon, name)
    })
    val stations = spark.createDataFrame(stationsRDD,sSchema).cache()
    data2020.createOrReplaceTempView("data2020")
    val pureSql = spark.sql(
      """
        |SELECT sid,date,value as tmax FROM data2020  WHERE mtype = 'TMAX'
      """.stripMargin)
//    pureSql.show()

    val tmax2020_t = data2020.filter($"mtype"==="TMAX").withColumnRenamed("value","tmax")
    val tmin2020_t = data2020.filter('mtype === "TMIN").withColumnRenamed("value","tmin")
    val tmax2020 = tmax2020_t.drop(col("mtype"))
    val tmin2020 = tmin2020_t.drop(col("mytpe"))
    val combined2020 = tmax2020.join(tmin2020,Seq("sid","date")).drop("mtype")
    val dailyTemp2020 = combined2020.select('sid,'date,('tmax+'tmin)/12*1.8+32).withColumnRenamed("((((tmax + tmin) / 12) * 1.8) + 32)","tave")
    val stationsTempData = dailyTemp2020.groupBy("sid").agg(avg("tave"))
    val joinData2020 = stationsTempData.join(stations,Seq("sid"))
//    joinData2020.show()

    spark.stop()
  }

}
