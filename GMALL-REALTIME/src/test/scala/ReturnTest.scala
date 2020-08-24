object ReturnTest {
  def main(args: Array[String]): Unit = {
    val array = Array(30, 50, 70, 80, 90, 10)
    // 匿名函数不能使用return

    // array.foreach(newreturn)

    try {
      array.foreach(x => {
        if (x > 50) return
        println(x)
      })
    } catch {
      case e =>
    }

  }

  def newreturn(x: Int): Unit = {
    if (x > 50)
      return
    println(x)
  }
}
