package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

class Spark01_RDD {

}
object Spark01_RDD{
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("SparkWC").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD
    //1.从内存中创建：makeRDD   底层实现就是parallelize
    val listRDD1: RDD[Int] = sc.makeRDD(List(1,2,3,4,5))

    //自定义分区，最后的数字3代表定义了3个分区，也就是最后保存到文件中只会出现三个文件
    val listRDD2: RDD[Int] = sc.makeRDD(List(1,2,3,4,5),3)

//    listRDD.collect().foreach(println)

    //2。从内存中创建：parallelize
    val arrayRDD: RDD[Int] = sc.parallelize(Array(1,2,3,4,5,6))

//    arrayRDD.collect().foreach(println)

    //3.从外部存储中创建
    //默认情况下可以读取项目路径，也可以读取其他路径：HDFS
    //默认从文件中读取的数据类型都是字符串类型
    //读取文件时，传递的分区参数为最小分区数，但不一定是这个分区数，取决于hadoop读取文件时的分片规则
    val filrRDD: RDD[String] = sc.textFile(".idea/input/word.txt")

    //将RDD的数据保存到文件中
    listRDD2.saveAsTextFile("output")



  }

}
