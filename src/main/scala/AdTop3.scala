import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
    统计出每一个省份广告被点击次数的TOP3
    数据文件 agent.log
    数据结构：时间戳，省份，城市，用户，广告，中间字段使用空格分割

 */
object AdTop3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("AdTop3")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("agent.log")
    lines.foreach(println)
    sc.stop()

  }

}
