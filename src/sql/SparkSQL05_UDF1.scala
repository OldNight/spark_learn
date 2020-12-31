package sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSQL05_UDF1 {
  //UDF：用户自定义函数
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")
    val spark:SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    //读取json文件后自定义一个函数，在字段前加上"Name"
    val frame: DataFrame = spark.read.json("input/input/user.json")
    spark.udf.register("addName",(x:String)=>"Name:"+x)
    frame.createOrReplaceTempView("people")
    spark.sql("Select name from people").show()

    spark.sql("Select addName(name) from people").show()

    spark.stop()

  }
}

