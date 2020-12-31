package sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL04_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")
    val spark:SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //进行转换之前，必须引入隐式转换规则
    //这里的spark是SparkSession的对象名称，而不是包名
    import spark.implicits._
    //创建RDD
    val rdd: RDD[(Int, String, String)] = spark.sparkContext.makeRDD(List((1,"dio","吸血鬼"),(2,"jojo","人类"),(1,"kazi","柱之男")))

    //RDD直接转换为DS
    val userRDD: RDD[User] = rdd.map {
      case (id, name, race) => {
        User(id, name, race)
      }
    }
    val userDS: Dataset[User] = userRDD.toDS()

    val rdd1: RDD[User] = userDS.rdd

    rdd1.foreach(println)


    spark.stop()

  }
}

