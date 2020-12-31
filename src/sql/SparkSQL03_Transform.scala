package sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL03_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")
    val spark:SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //进行转换之前，必须引入隐式转换规则
    //这里的spark是SparkSession的对象名称，而不是包名
    import spark.implicits._
    //创建RDD
    val rdd: RDD[(Int, String, String)] = spark.sparkContext.makeRDD(List((1,"dio","吸血鬼"),(2,"jojo","人类"),(1,"kazi","柱之男")))

    //转换为DF
    val df: DataFrame = rdd.toDF("id","name","race")

    //转换为DS
    val ds: Dataset[User] = df.as[User]

    //转换为DF
    val df1: DataFrame = ds.toDF()

    //转换为RDD
    val rdd1: RDD[Row] = df1.rdd

    rdd1.foreach(row=>{
      //获取数据时，可以通过索引访问数据
      println(row.getString(1))
    })

    spark.stop()

  }
}
//DF转换为DS时需要一个样例类
case class User(id:Int,name:String,race:String)
