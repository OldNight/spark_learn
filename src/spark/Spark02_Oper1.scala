package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkWC").setMaster("local[*]")
    //上下文对象
    val sc: SparkContext = new SparkContext(conf)

    //mapPartitions算子
    val listRDD: RDD[Int] = sc.makeRDD((1 to 10))

    //mapPartitions可以对一个RDD中的所有分区进行遍历
    //mapPartitions效率优于map算子，减少了发送到执行器执行交互的次数
    //mapPartitions可能会出现内存溢出（OOM）
    val mapPartitionsRDD: RDD[Int] = listRDD.mapPartitions(datas=>{
      datas.map(data=>data*2)
    })

    mapPartitionsRDD.collect().foreach(println)


  }
}
