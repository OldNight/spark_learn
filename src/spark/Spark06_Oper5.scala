package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Oper5 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkWC").setMaster("local[*]")
    //上下文对象
    val sc: SparkContext = new SparkContext(conf)


    val listRDD: RDD[Int] = sc.makeRDD(1 to 16,4)

    //讲一个分区的数组放入一个数组中
    val glomRDD: RDD[Array[Int]] = listRDD.glom()

    glomRDD.collect().foreach(array=>{
      println(array.mkString(","))
    })


  }
}
