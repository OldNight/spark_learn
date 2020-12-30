//package spark
//
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}
//
//object Spark11_Oper10 {
//  def main(args: Array[String]): Unit = {
//    val conf: SparkConf = new SparkConf().setAppName("SparkWC").setMaster("local[*]")
//    //上下文对象
//    val sc: SparkContext = new SparkContext(conf)
//
//    // 减少分区，eg:将十个分区变成9个时，可以使用合并算子coalesce将最后的两个分区合并成一个
//    //当想要将10个分区减少城2个分区时，合并算子就不适用了，因为会造成数据倾斜
//    //所以要使用repartition算子(有shuffle操作，而coalesce可选择是否有shuffle操作)
//    //repartition实际上就是调用coalesce算子，区别就是repartition默认shuffle为true
//    val listRDD: RDD[Int] = sc.makeRDD(1 to 16,4)
//
//
//
//    groupByRDD.collect().foreach(println)
//  }
//}
