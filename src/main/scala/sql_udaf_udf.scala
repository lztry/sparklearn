import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Test

class sql_udaf_udf {
  @Test
  def sparkSql = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSql")
    val spark: SparkSession = SparkSession.
      builder().
      config(conf).
      getOrCreate()
    //隐式转换
    val df: DataFrame = spark.read.format("json").load("in/user.json")
    //创建临时表 sql 风格
    // createOrReplaceTempView 只是一次session 范围内，session范围内有效
    //view 只能查不能改
    df.createOrReplaceTempView("user")
    //查询
    spark.sql("select * from user").show()


  }

}
