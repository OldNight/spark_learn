package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkWC").setMaster("local[*]")
    //上下文对象
    val sc: SparkContext = new SparkContext(conf)

    //map算子
    val listRDD: RDD[Int] = sc.makeRDD((1 to 10))

    val mapRDD: RDD[Int] = listRDD.map(x=>x*2)
//    val mapRDD: RDD[Int] = listRDD.map( _*2 )

    mapRDD.collect().foreach(println)


  }
}
