import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class accumulation {
  @Test
  def testAccmulator = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("accmulator")
    val sc = new SparkContext(sparkConf)
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //accmulator 工作节点上的任务不能访问累加器的值
    //创建累加器
    val accumulator: LongAccumulator = sc.longAccumulator
    dataRDD.foreach(accumulator.add(_))
    println(accumulator.value)
    sc.stop()
  }

  @Test
  def defineAccmulator = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("accmulator")
    val sc = new SparkContext(sparkConf)
    val dataRDD: RDD[String] = sc.makeRDD(List("1h", "2", "3", "4h"))

    val accumulator: WordAccumulator = new WordAccumulator
    //注册累加器]
    sc.register(accumulator)
    dataRDD.foreach(accumulator.add(_))
    println(accumulator.value)
    sc.stop()
  }
}

class WordAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {
  val list = new util.ArrayList[String]()

  //当前累加器是否初始化
  override def isZero: Boolean = list.isEmpty

  //复制累加器对象
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new WordAccumulator()
  }

  //重置累加器
  override def reset(): Unit = list.clear

  //累加器增加数据
  override def add(v: String): Unit = {
    if (v.contains('h'))
      list.add(v)
  }

  //合并
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list addAll other.value
  }

  override def value: util.ArrayList[String] = list
}