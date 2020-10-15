import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Before, Test}

class Spark_Operation {
  var conf:SparkConf = null
  var sc:SparkContext = null
  @Before
  def init()={
    conf = new SparkConf().setMaster("local[*]").setAppName("rddPart")
    sc = new SparkContext(conf)
  }
  @Test
  def glom()={
    val listRDD: RDD[Int] = sc.makeRDD(1 to 20,2)
    //glom 将每一个分区形成一个数组，形成新的RDD类型时RDD[Array[T]]
    val glomRDD: RDD[Array[Int]] = listRDD.glom()
    glomRDD.collect().foreach(datas=>{
      println(datas.mkString(","))
    })
  }
  @Test
  def groupBy()={

    val listRDD: RDD[Int] = sc.makeRDD(1 to 20,2)
    //groupBy 分组，按照传入函数进行分组 按照模2 分组。
    // 分组后的元素形成元祖(K-V), K 为 分组key ,V 表示集合
    val groupByRDD: RDD[(Int, Iterable[Int])] = listRDD.groupBy(i=>i%2)
    groupByRDD.collect().foreach(println)
  }
  @Test
  def filter={
    val listRDD: RDD[Int] = sc.makeRDD(1 to 20,2)
    //filter 过滤 true 留，false 溜
    val filterRDD: RDD[Int] = listRDD.filter(x=>x%2==0)
    filterRDD.collect().foreach(println)
  }
  @Test
  def sample={
    val listRDD: RDD[Int] = sc.makeRDD(1 to 20,2)
    //sample 抽样 在发生数据热点 数据不平衡问题时用来探测。
    /*
         withReplacement表示是抽出的数据是否放回，true为有放回的抽样，false为无放回的抽样，
        seed用于指定随机数生成器种子。
        fraction 当为无放回的时候，表示每个元素选择的几率，当为true的时候表示每个元素选择的次数
     */

    val sampleRDD: RDD[Int] = listRDD.sample(true,2)
    sampleRDD.collect().foreach(println)
  }
  @Test
  def distinct: Unit = {
    val listRDD: RDD[Int] = sc.makeRDD(1 to 20,2)
    //distinct RDD进行去重
    val repeatListRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,4,4,4,6,6,1,6,6))
    // numPartitions 表示分区数，去重后分区数可能减少，借此减少分区数
    val distinctRDD: RDD[Int] = repeatListRDD.distinct()
    //输出结果和输入顺序不一致，distinct为shuffle 操作，会把数据打乱 重组到其他分区
    //明显效率低
    distinctRDD.collect().foreach(println)
  }
}
