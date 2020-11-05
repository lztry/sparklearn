package syntax

import org.junit.Test

class grammer {
  //参数化类型，泛型
  def echo[T](input1: T, input2: T): Unit = {
    println(s"input1: $input1,input2:$input2")
  }

  @Test
  def call(): Unit = {
    echo(1, 2)
    //scala 中类都继承Any,所以能任何类似都能传入
    echo("22", 22)
    echo[String]("22", "33")
  }

}

class airPlane(location: Int) {
  println(s"location:$location")

  def this(location: Int, name: String) {
    //辅助构造函数必须调用祝构造函数
    this(location)
    println(s"name is $name")
  }
}