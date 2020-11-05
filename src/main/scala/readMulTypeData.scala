import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Before, Test}

import scala.util.parsing.json.JSON

class readMulTypeData {
  var sc: SparkContext = null

  @Before
  def init = {
    val sparkConf = new SparkConf().setAppName("read").setMaster("local[*]")
    sc = new SparkContext(sparkConf)
  }

  @Test
  def readJson = {
    // 读取json文件，读取文件后转换为JSON 数据
    //使用RDD读取JSON文件处理很复杂，同时SparkSQL集成了很好的处理JSON文件的方式，所以应用中多是采用SparkSQL处理JSON文件
    val fileRDD: RDD[String] = sc.textFile("user.json")
    fileRDD.map(JSON.parseFull).collect().foreach(println)
  }

  @Test
  def readMysql: Unit = {
    //通过Java JDBC访问关系型数据库。需要通过JdbcRDD进行
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/db1"
    val userName = "root"
    val passWd = "root"
    val mysqlRDD = new JdbcRDD(sc, () => {
      Class.forName(driver)
      DriverManager.getConnection(url, userName, passWd)
    },
      "select * from student where id >=? AND id<=?",
      1,
      10,
      4,
      //对行进行操作
      (rs) => {
        (rs.getString(2), rs.getInt(3), rs.getString(4))
      }
    )
    mysqlRDD.collect().foreach(println)
  }

  @Test
  def mysqlDataSave: Unit = {
    //数据库中插入顺序和遍历顺序不一致，由executor执行，顺序不确定
    val userRDD: RDD[(String, Int, String)] = sc.makeRDD(List(("三1", 2, "女"), ("三2", 2, "女"), ("三3", 2, "女"), ("三4", 2, "女")))
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/db1"
    val userName = "root"
    val passWd = "root"
    //插入数据库 存在缺点，一条数据就得创建一次连接，效率低还可能导致Mysql崩溃
    userRDD.foreach {
      case (name, age, sex) => {
        Class.forName(driver)
        val connection: Connection = DriverManager.getConnection(url, userName, passWd)
        val sql = "insert into student (name,age,sex) values(?,?,?)"
        val statement: PreparedStatement = connection.prepareStatement(sql)
        statement.setString(1, name)
        statement.setInt(2, age)
        statement.setString(3, sex)
        statement.executeUpdate()
        //关闭连接
        statement.close()
        connection.close()
      }
    }
  }

  @Test
  def mysqlDataSaveOptimize = {
    //对原来的插入进行优化，用foreachParttiton,一个分区连接一次数据库
    //缺点 可能出现内存溢出，一
    val userRDD: RDD[(String, Int, String)] = sc.makeRDD(List(("三1", 2, "女"), ("三2", 2, "女"), ("三3", 2, "女"), ("三4", 2, "女")))
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/db1"
    val userName = "root"
    val passWd = "root"
    userRDD.foreachPartition(datas => {
      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url, userName, passWd)
      val sql = "insert into student (name,age,sex) values(?,?,?)"
      val statement: PreparedStatement = connection.prepareStatement(sql)
      datas.foreach {
        case (name, age, sex) => {
          statement.setString(1, name)
          statement.setInt(2, age)
          statement.setString(3, sex)
          statement.executeUpdate()
        }
      }
      statement.close()
      connection.close()
    })
  }
}
