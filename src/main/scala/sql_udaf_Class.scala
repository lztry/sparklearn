import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}
import org.junit.Test

class sql_udaf_Class {
  @Test
  def trans = {
    val conf: SparkConf = new SparkConf().setAppName("Spark_SQL").setMaster("local[*]")
    val spark = SparkSession.
      builder().
      config(conf).
      getOrCreate()
    //隐式转换，spark 为对象名称
    val udaf = new myAgeAvgClassFuntion

    //spark.udf.register("myave",test)

    val df: DataFrame = spark.read.json("in/user.json")
    df.createOrReplaceGlobalTempView("user")
    df.select("select udaf()")
  }

}

case class AvgBuffer(var sum: BigInt, var count: Int)

class myAgeAvgClassFuntion extends Aggregator[Int, AvgBuffer, Double] {
  //初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0, 0)
  }

  override def reduce(b: AvgBuffer, a: Int): AvgBuffer = {
    b.count += 1
    b.sum += a
    b
  }

  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}