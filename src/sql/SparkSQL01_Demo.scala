package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL01_Demo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")
    val spark:SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val frame2: DataFrame = spark.read.json("input/user.json")
    frame2.show()
    spark.stop()

  }
}
