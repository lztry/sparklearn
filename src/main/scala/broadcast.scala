import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class broadcast {
  @Test
  def broadcastTest: Unit = {
    //广播变量用来高效分发较大的对象。向所有工作节点发送一个较大的只读值，以供一个或多个Spark操作使用
    //凡是用到driver变量都会被封装到task 中。而广播变量只向executor 分发一次提高效率
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("accmulator")
    val sc = new SparkContext(sparkConf)
    val intRDD = sc.makeRDD(List((1, 1), (2, 2), (3, 3), (4, 4)))
    val list = List((1, 'a'), (2, 'b'), (3, 'c'))
    val broadcastList: Broadcast[List[(Int, Char)]] = sc.broadcast(list)
    val resultRDD: RDD[Unit] = intRDD.map(data => {
      for (elem <- broadcastList.value) {
        if (data._1 == elem._1)
          (data._1, (data._1, elem._2))
      }
    })
    resultRDD.collect().foreach(println)


  }

}

