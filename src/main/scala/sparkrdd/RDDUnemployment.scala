package sparkrdd

import org.apache.spark.sql.SparkSession

case class Area(code:String,text:String)
case class Series(id:String, area:String,measure :String ,title:String)
case class LAData(id:String,year:Int,period:Int,value: Double)


object RDDUnemployment {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .master("local")
      .appName("BigData with RDD Unemployment")
      .getOrCreate()
    spark.sparkContext.setLogLevel("Error")

    val areas = spark.sparkContext
      .textFile("data/la.area.txt")
      .filter(!_.contains("area_text"))
      .map{line => {
        val p =line.split("\t").map(_.trim)
        Area(p(1),p(2))
      }}.cache()

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
    val rates = data.filter(_.id.endsWith("03"))
    val decadeGroups = rates.map(d => (d.id,d.year/10) -> d.value)
    val decadeAverages = decadeGroups.aggregateByKey((0.0,0))({
      case ((s,c),v) => (s+v,c+1)
    },{case ((s1,c1),(s2,c2)) => (s1+s2,c1+c2)}).mapValues(t => t._1/t._2)

    val maxDecadeBySeries = decadeAverages.map{case ((id,dec),av) => id -> (dec,av)}
      .reduceByKey{case((d1,a1),(d2,a2)) => if (a1 >= a2) (d2,a2) else (d1,a1)}
    val seriesPairs = series.map(s=>s.id -> s.title)
    val joinMaxDecades = seriesPairs.join(maxDecadeBySeries).mapValues{case (a,(b,c)) => (a,b,c)}
    val dataByArea = joinMaxDecades.map{case(id,t) => (id.drop(3).dropRight(2),t)}
    dataByArea take(5) foreach println
    val fullyJoined = areas.map(a=>a.code->a.text).join(dataByArea)
    fullyJoined take(5) foreach println


  }
}
