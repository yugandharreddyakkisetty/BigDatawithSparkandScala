package sparkWindowUDF

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoders._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrameNaFunctions
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import java.sql.Timestamp

case class Employee(Name:String,Account:String,EmpID:String,Doj:Timestamp)
object WindowUDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("WidnowsUDF").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    val sales=Seq(("KTM",20,"2017-01-10",100),
      ("KTM",30,"2017-01-11",110),
      ("KTM",32,"2017-01-12",109),
      ("Pulser",22,"2017-01-10",110),
      ("Pulser",24,"2017-01-11",99),
      ("Pulser",26,"2017-01-12",111),
      ("Aprilla",11,"2017-01-10",89),
      ("Aprilla",12,"2017-01-11",91),
      ("Aprilla",13,"2017-01-12",95) ).toDF("Bike_type","Units Sold","Date_of_Sale","Profit")


    sales.show()
    // calculating cumulative sum and average of profit
    val wSpec = Window.partitionBy(col("Bike_type")).orderBy(col("Date_of_Sale")).rangeBetween(Window.unboundedPreceding,Window.currentRow)
    sales.withColumn("cumulative_sum",sum('Profit).over(wSpec))
      .withColumn("cumulative_average",avg('Profit).over(wSpec))
      .show()

    // ranking sales days
    val rankingWindow = Window.partitionBy('Bike_type).orderBy('Profit.desc)
    sales.withColumn("Rank",rank().over(rankingWindow)).show()

    // lead and lag functions

    val leadLagWindow = Window.partitionBy($"Bike_type").orderBy(col("Date_of_Sale"))
    // Previous days sales
    sales.withColumn("Lastday_sales",lag(col("Units Sold"),1,100).over(leadLagWindow)).show()
    // Next day sales
    sales.withColumn("Next Day Sales",lead($"Units Sold",1,100).over(leadLagWindow)).show()

    //DataFrameNaFunctions
    println("DataFrameNaFunctions")
    sales.na.replace(Seq("Units Sold"),Map[Int,Int](2->20,11->111)).show()
    sales.na.drop(0,Seq("Units Sold")).show()


    // UDFs

    val employees=Seq(("Yugandhar","LBG","2001",100,"2017-10-10"),
      ("Nagesh","LBG","2002",102,"2016-10-10"),
      ("Jaya","LBG","2003",102,"2015-10-10"),
      ("Pawan","ANZ","2004",97,"2014-10-10"),
      ("Charan","ANZ","2005",106,"2014-10-09"),
      ("Jaya","LBG","2003",102,"2015-10-10"),
      ("Chandra","ANZ","2006",20,"2013-10-10")
    ).toDF("Name","Account","EmpID","Salary","Doj")
/*

    // udf to generate email address of a employee
    val generateEmail = udf { (name:String,account:String)=>name+"@"+account+".com"}

    employees.withColumn("Email",generateEmail(col("Name"),col("Account"))).show()
    employees.select(generateEmail('Name,'Account)).show()
/*
  At this juncture, we can use this only as a DataFrame function. That is to say, we can’t use it
  within a string expression, only on an expression. However, we can also register this UDF as a
  Spark SQL function. This is valuable because it makes it simple to use this function within SQL
  as well as across languages.
*/
    spark.udf.register("generateEmpEmail",generateEmail)
    employees.selectExpr("generateEmpEmail(Name,Account)").show()


    // UDF -2
    // Complex UDF
    // sums employee id and his salary
    val sumUDF = udf {
      (arr: Seq[Row]) => {
        val brr = arr.map(x => Row(x.getAs[Int](0), x.getAs[Int](1)))
        var kount = 0
        for (i <- brr) {
          kount = kount + i(0).toString.toInt + i(1).toString.toInt
        }
        kount
      }
    }

        employees
      .groupBy('Account)
      .agg(collect_list(struct('Salary,'EmpID)) as "salary_empid_list")
      .withColumn("sumUDF",sumUDF($"salary_empid_list")).show()

    // UDF -3

    val averageUDF = udf{
      (arr:Seq[Int]) => {
        arr.sum/arr.length
      }
    }
    employees.groupBy($"Account").agg(collect_list('Salary) as "salary_list").withColumn("Average Salary",averageUDF(col("salary_list"))).show()

    // Working with date and time
    //first converst Doj  to DateTpye and find the difference
    employees.withColumn("Doj",to_date('Doj,"yyyy-MM-dd")).withColumn("Exp",datediff(current_date,'Doj)).show()
*/

    // date to timestamp and timestamp to date conversion
    employees.printSchema()
    employees.show()
    employees
      .withColumn("Timestamp",unix_timestamp(col("Doj"),"yyyy-MM-dd"))
      .withColumn("Date_from_ut",from_unixtime(col("Timestamp")))
      .show()


  }

}
