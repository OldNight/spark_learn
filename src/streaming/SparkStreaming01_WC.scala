package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_WC {
  def main(args: Array[String]): Unit = {
    //使用spark streaming完成WC
    //spark配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WC")
    //实时数据分析环境对象
    //采集周期：以指定时间为周期采集数据
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    //从指定端口采集数据，ps：需要启动hadoop服务，并且安装有netcat，输入指令nv -lk 9999，就可以往指定端口发送数据
    val socketLineDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("192.168.199.137",9999)
    //采集数据进行分解（扁平化）
    val wordDStream: DStream[String] = socketLineDStream.flatMap(line=>line.split(" "))
    //将数据进行结构转变
    val mapDStream: DStream[(String, Int)] = wordDStream.map((_,1))

    val wordToSumDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_+_)

    wordToSumDStream.print()

    //启动采集器
    streamingContext.start()
    //Drvier等待采集器的执行
    streamingContext.awaitTermination()







  }
}
