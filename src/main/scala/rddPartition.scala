import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Before, Test}

/**
 *  测试RDD分区
 *  分区规则和填数据的规则不一致
 *  hadoop 都是按行进行读取。
 */
class rddPartition {
  var conf:SparkConf = null
  var sc:SparkContext = null
  @Before
  def init()={
    conf = new SparkConf().setMaster("local[2]").setAppName("rddPart")
    sc = new SparkContext(conf)
  }
  @Test
  def  makeRDD2File={
    // makeRDD底层实现为parallelize  defaultParallelism默认分区数
    val listRDD:RDD[Int] = sc.makeRDD(List(1,2,3,4,5))
    //写入文件测试分区 因为有2个线程所以创建2个分区生成2 个文件
    listRDD.saveAsTextFile("output")
  }
  @Test
  def fileTest()={
    //测试文件分区  defaultMinPartitions默认分区数 和内存不同的是math.min(defaultParallelism, 2)取小值，内存分区取大值
    //为什么这么设计呢?因为文件大小不确定，规定最小的分区，可以进行扩展
    var fileRDD = sc.textFile("wordcount")
    // 保存到文件中
    fileRDD.saveAsTextFile("file_output")
  }
  @Test
  def minPartFile={
    /*
       测试分区值，读取文件时为最小分区数，不一定为这个数。取决于hadoop 的分片规则
    */
    //为什么行不能整除不会多分区呢？ 考虑1.1的情况
    val fileRDDTwo = sc.textFile("wordcount",2)
    //实验结果创建了四个分区，读文件的方式是用hadoop读文件的方式。hadoop切片
    fileRDDTwo.saveAsTextFile("file_output_value")
  }
}
