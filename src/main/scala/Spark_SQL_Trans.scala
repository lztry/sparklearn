import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.junit.Test

class Spark_SQL_Trans {
  @Test
  def trans = {
    val conf: SparkConf = new SparkConf().setAppName("Spark_SQL").setMaster("local[*]")
    val spark = SparkSession.
      builder().
      config(conf).
      getOrCreate()
    //隐式转换，spark 为对象名称
    import spark.implicits._
    val userRDD: RDD[(String, Int, Int)] = spark.sparkContext.makeRDD(List(("lz", 2, 122), ("lzl", 2, 122), ("lzlzl", 2, 122), ("lzzllzzl", 2, 122)))
    //rdd to df
    val df: DataFrame = userRDD.toDF("name", "id", "money")
    //df to ds
    val ds: Dataset[User] = df.as[User]
    //df to rdd
    val rdd: RDD[Row] = df.rdd
    rdd.foreach(row => {
      //获取数据，通过索引得到数据
      println(row.getString(0))
    })
    //rdd to ds
    val userDs: Dataset[User] = userRDD.map {
      case (name, id, money) => User(name, id, money)
    }.toDS()
    //ds to rdd
    val userDs2RDD: RDD[User] = userDs.rdd
    //ds to df
    val userDf: DataFrame = userDs.toDF()
    userDf.show()
    spark.stop()
  }

}

//样例类
case class User(name: String, id: Int, money: Int)
