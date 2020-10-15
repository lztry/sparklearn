import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Driver_Executor {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Driver_Executor").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    //i 在driver,在Executor执行时，需要把i 传递到Executor，传递过程需要进行 IO 序列化
    // 如果不能序列化则出错
    var i =10;
    //所有的RDD算子的计算功能都由Executor执行
    val listRDD: RDD[Int] = sc.makeRDD(1 until(10)  ) //until 最后一个取不到
    val mapRDD = listRDD.map(_ * i)
    mapRDD.collect().foreach(println)
    
  }

}
