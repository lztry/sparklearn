import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
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

  @Test
  def coalesce = {
    //减少分区数 合并分区 只能减少不能增加
    val listRDD: RDD[Int] = sc.makeRDD(1 to 20, 5);
    listRDD.saveAsTextFile("output1")
    println(listRDD.partitions.size)
    //默认shuffle为false,shuffle慢需要将内容写到中间文件然后读文件到分区
    //没有shuffle一个分区可以认为就是task,有了读写task的数量会增多，读task和写task

    //println(listRDD.coalesce(2).partitions.size)
    //coalesce 将分区数据复制到其他分区,可能会发生数据倾斜
    listRDD.coalesce(2).saveAsTextFile("output2")
  }

  @Test
  def repartition(): Unit = {
    //repartition 重新分区 shuffle 改变分区数量 可多可少
    val listRDD: RDD[Int] = sc.makeRDD(1 to 20, 4);
    //底层实现coalesce(numPartitions, shuffle = true) 打乱重组到分区
    //println(listRDD.repartition(5).partitions.size)
    listRDD.repartition(5).saveAsTextFile("output")
  }

  @Test
  def sortBy = {
    //排序 根据处理结果 排序 原始数据
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 3, -1, 3, 3, 5, 2, 0), 4);
    //默认从小到大
    val sortByRDD: RDD[Int] = listRDD.sortBy(x => x)
    sortByRDD.collect().foreach(println)
  }

  /*
    RDD 之间的交互
   */
  @Test
  def union = {
    // 求并集
    val listRDD1: RDD[Int] = sc.makeRDD(1 to 5)
    val listRDD2: RDD[Int] = sc.makeRDD(6 until (9))
    val listRDD3: RDD[String] = sc.makeRDD(List("111", "23"))
    val unionRDD: RDD[Int] = listRDD1.union(listRDD2)
    unionRDD.collect().foreach(println)
  }

  @Test
  def subtract = {
    //求差集
    val listRDD1: RDD[Int] = sc.makeRDD(1 to 5)
    val listRDD2: RDD[Int] = sc.makeRDD(List(1, 4, 5, 6, 7))
    val subtractRDD: RDD[Int] = listRDD1.subtract(listRDD2)
    subtractRDD.collect().foreach(println)
  }

  @Test
  def intersection = {
    //求交集
    val listRDD1: RDD[Int] = sc.makeRDD(1 to 5)
    val listRDD2: RDD[Int] = sc.makeRDD(List(1, 4, 5, 6, 7))
    val intersectionRDD: RDD[Int] = listRDD1.intersection(listRDD2)
    intersectionRDD.foreach(println)

  }

  @Test
  def cartesian = {
    //笛卡尔积
    val listRDD1: RDD[Int] = sc.makeRDD(1 to 5)
    val listRDD2: RDD[Int] = sc.makeRDD(List(1, 4, 5, 6, 7))
    val cartesianRDD: RDD[(Int, Int)] = listRDD1.cartesian(listRDD2)
    cartesianRDD.collect().foreach(println)
  }

  @Test
  def zip = {
    //拉链 ，形成K-V 对 要求partition和数据相同
    val listRDD1: RDD[Int] = sc.makeRDD(1 to 5)
    val listRDD2: RDD[Char] = sc.makeRDD('a' to 'e')
    val zipRDD: RDD[(Char, Int)] = listRDD2.zip(listRDD1)
    zipRDD.collect().foreach(println)
  }

  /*
   处理key-value 类型数据
   */
  @Test
  def partitionBy = {
    //通过key 进行分区
    val listRDD1: RDD[Int] = sc.makeRDD(1 to 5)
    val listRDD2: RDD[Char] = sc.makeRDD('a' to 'e')
    val zipRDD: RDD[(Int, Char)] = listRDD1.zip(listRDD2)
    zipRDD.mapPartitionsWithIndex {
      case (num, datas) => {
        datas.map((num, _))
      }
    }.collect().foreach(println)
    println("---------------重新分区-----------------")
    //HashPartitioner 分区方式是对分区数进行求模,与hashmap 不同,Hashmap 使用& ，也可以自定义分区器
    //val partitionRDD: RDD[(Int, Char)] = zipRDD.partitionBy(new HashPartitioner(2))
    //partitionRDD.mapPartitionsWithIndex((num,datas)=>{datas.map((num,_))}).collect().foreach(println)
    val partitionRDD: RDD[(Int, Char)] = zipRDD.partitionBy(new myPartitioner(3))
    partitionRDD.mapPartitionsWithIndex((num, datas) => {
      datas.map((num, _))
    }).collect().foreach(println)

  }

  @Test
  def groupByKey = {
    //groupByKey 根据key 进行分组
    val words = Array("one", "two", "two", "three", "three", "three")
    val wordRDD: RDD[(String, Int)] = sc.makeRDD(words).map((_, 1))
    val groupByKeyRDD: RDD[(String, Iterable[Int])] = wordRDD.groupByKey()
    groupByKeyRDD.map(t => (t._1, t._2.sum)).collect().foreach(println)
  }

  @Test
  def reduceByKey = {
    /*
     1. reduceByKey：按照key进行聚合，在shuffle之前有combine（预聚合）操作，返回结果是RDD[k,v].
     2. groupByKey：按照key进行分组，直接进行shuffle。
     */
    //reduceByKey 将相同key的值聚合到一起,参数表示 两两如何操作
    val words = Array("one", "two", "two", "three", "three", "three")
    val wordRDD: RDD[(String, Int)] = sc.makeRDD(words).map((_, 1))
    val value: RDD[(String, Int)] = wordRDD.reduceByKey(_ + _)
    value.collect().foreach(println)
  }

  @Test
  def aggregateByKey = {
    //aggregateByKey 可以自定义分区内和分区间的操作，相当于可以自定义combine 和  reduce
    // 任务： 创建一个pairRDD，取出每个分区相同key对应值的最大值，然后相加
    val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
    val aggregateByKeyRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(math.max(_, _), (_ + _))
    //aggregateByKeyRDD.collect().foreach(println)
    //wordcount
    rdd.aggregateByKey(0)((_ + _), (_ + _)).collect().foreach(println)

  }

  @Test
  def foldByKey = {
    //foldByKey seqOp = combOp
    val words = Array("one", "two", "two", "three", "three", "three")
    val wordRDD: RDD[(String, Int)] = sc.makeRDD(words).map((_, 1))
    val foldByKeyRDD: RDD[(String, Int)] = wordRDD.foldByKey(0)(_ + _)
    foldByKeyRDD.collect().foreach(println)
  }

  @Test
  def combineByKey = {
    //combineByKey 与aggregateByKey 功能相似不同的是combineByKey 转变第一个 value 数据的数据类型
    val input = sc.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2)
    //根据key计算每种key的均值
    //input.groupByKey().map(t=>(t._1,t._2.sum/t._2.size.toDouble)).collect().foreach(println)
    //使用combineByKey 注意，转变的类型不能自动推断，需要声明类型
    val combineByKeyRDD: RDD[(String, (Int, Int))] = input.combineByKey((_, 1),
      (data: (Int, Int), v) => (data._1 + v, data._2 + 1), //分区间计算 ，计算完毕后数据都变为元祖
      (data1: (Int, Int), data2: (Int, Int)) => (data1._1 + data2._1, data1._2 + data2._2))
    //val resultRDD: RDD[(String, Double)] = combineByKeyRDD.map{case (key,value)=>(key,value._1/value._2.toDouble)}
    val resultRDD: RDD[(String, Double)] = combineByKeyRDD.mapValues((v) => (v._1 / v._2.toDouble))
    // combineByKeyRDD.map(case (key,value)=>(key,value._1,value._2)) 无法推断类型
    resultRDD.collect().foreach(println)
  }

  @Test
  def sortBykey = {
    //sortByKey 通过K 排序 K必须实现Ordered接口
    val rdd: RDD[(Int, String)] = sc.parallelize(Array((3, "aa"), (6, "cc"), (2, "bb"), (1, "dd")))
    //true 从小到大
    rdd.sortByKey(true).collect().foreach(println)
  }

  @Test
  def mapValues = {
    //mapValues 对value 进行操作
    val rdd: RDD[(Int, String)] = sc.parallelize(Array((1, "a"), (1, "d"), (2, "b"), (3, "c")))
    rdd.mapValues(_ + "|||").collect().foreach(println)
  }

  @Test
  def join = {
    //join 在类型为(K,V)和(K,W)的RDD上调用，返回一个相同key对应的所有元素对在一起的(K,(V,W))的RDD
    //只返回二者共有的key
    val rdd1 = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c")))
    val rdd2 = sc.parallelize(Array((1, 4), (2, 5), (3, 6), (8, 8)))
    val joinRDD = rdd1.join(rdd2).join(rdd2)
    joinRDD.collect().foreach(println)
  }

  @Test
  def cogroup = {
    //cogroup 在类型为(K,V)和(K,W)的RDD上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的RDD
    // 没有的key补null,分左右
    val rdd1 = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c")))
    val rdd2 = sc.parallelize(Array((1, 4), (2, 5), (3, 6), (8, 8)))
    val cogroupRDD1: RDD[(Int, (Iterable[String], Iterable[Int]))] = rdd1.cogroup(rdd2)
    cogroupRDD1.collect().foreach(println)
    rdd2.cogroup(rdd1).collect().foreach(println)
  }

}

//自定义分区器
class myPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    1
  }
}

