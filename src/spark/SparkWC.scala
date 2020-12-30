package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWC {
  def main(args: Array[String]): Unit = {

    //配置信息类
    val conf: SparkConf = new SparkConf().setAppName("SparkWC").setMaster("local[*]")
    //上下文对象
    val sc: SparkContext = new SparkContext(conf)
    println(sc)

    //读取数据
    val lines: RDD[String] = sc.textFile(".idea/input/word.txt")
    //处理数据
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val paired: RDD[(String, Int)] = words.map((_,1))
    val reduced: RDD[(String, Int)] = paired.reduceByKey(_+_)
    //val res: RDD[(String, Int)] = reduced.sortBy(_._2,false)
    //保存
    //    res.saveAsTextFile((args(1)))
    val result = reduced.collect()
//    println(result.toBuffer)
    result.foreach(println)
//    println(res.collect().toBuffer)
    //结束任务
    sc.stop()
  }
}
