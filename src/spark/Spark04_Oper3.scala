package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Oper3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkWC").setMaster("local[*]")
    //上下文对象
    val sc: SparkContext = new SparkContext(conf)

    //mapPartitionsWithIndex算子
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10,2)

    val tupleRDD: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex {
      case (num, datas) => {
        datas.map((_, "分区号：" + num))
      }
    }


    tupleRDD.collect().foreach(println)


  }
}
