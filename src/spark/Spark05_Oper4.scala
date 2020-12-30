package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Oper4 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkWC").setMaster("local[*]")
    //上下文对象
    val sc: SparkContext = new SparkContext(conf)

    //mapPartitionsWithIndex算子
    val listRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1,2),List(3,4)))

    //flatMap
    //1,2,3,4
    val flatMapRDD: RDD[Int] = listRDD.flatMap(datas=>datas)


    flatMapRDD.collect().foreach(println)


  }
}
