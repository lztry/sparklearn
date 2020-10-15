import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Before, Test}

class Spark_Oper_Map {
  var conf:SparkConf = null
  var sc:SparkContext = null
  @Before
  def init()={
    conf = new SparkConf().setMaster("local[2]").setAppName("rddPart")
    sc = new SparkContext(conf)
  }
  @Test
  def map={
    val listRDD = sc.makeRDD(1 to 10)
    //map 每个数据都会进行计算
    val mapRDD: RDD[Int] = listRDD.map(_*2)
    mapRDD.collect().foreach(println)

  }
  @Test
  def mapPartitions={
    val listRDD = sc.makeRDD(1 to 10)
    //mapPartitions和 map不同，分区进行计算，效率更高，减少了IO,但是可能发生内存溢出
    val mapPartitionsRDD: RDD[Int] = listRDD.mapPartitions(_.map(_*2))
    //等价于
    //val mapPartitionsRDD: RDD[Int] = listRDD.mapPartitions(datas=>{datas.map(_*2)})
    mapPartitionsRDD.collect().foreach(println)
  }
  @Test
  def mapPartitionsWithIndex={
    val listRDD = sc.makeRDD(1 to 10)
    //mapPartitionsWithIndex 与分区有关得到分区号
    /*
    val tupleRDD: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex {
      case (num, datas) => {  //这里的case 不太明白
        datas.map((_, "分区号" + num))
      }
    }
    */
    val tupleRDD: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex((num,datas)=>{datas.map((_,"分区号"+num))})
    tupleRDD.collect().foreach(println)
  }
  @Test
  def main: Unit = {
    //flatMap 扁平化 只能解开一层
    val arrayRDD: RDD[List[Any]] = sc.makeRDD(Array(List(List(1,2),List(3,4)),List(5,6)))
    val flatMapRDD: RDD[Any] = arrayRDD.flatMap(datas=>datas)
    flatMapRDD.collect().foreach(println)
  }

}
