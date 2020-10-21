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
    //格式化数据
    val provinceAdToOne: RDD[((String, String), Int)] = lines.map(x => {
      val strings: Array[String] = x.split(" ")
      //2 和 5 为所需数据，转成元祖，计算数量
      ((strings(1), strings(4)), 1)
    })
    //广告求和
    val provinceAdToSum: RDD[((String, String), Int)] = provinceAdToOne.reduceByKey(_ + _)
    //通过省份聚合
    val groupByProvince: RDD[(String, Iterable[(String, Int)])] = provinceAdToSum.map {
      case (provinceAd, num) => {
        (provinceAd._1, (provinceAd._2, num))
      }
    }.groupByKey()
    //排序取前三
    val provinceAdTop3: RDD[(String, List[(String, Int)])] = groupByProvince.mapValues(x => {
      x.toList.sortWith((x, y) => x._2 > y._2)
      }.take(3))
    provinceAdTop3.collect().foreach(println)
    sc.stop()
  }
}
