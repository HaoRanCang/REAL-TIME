import org.json4s.{DefaultFormats, JValue}
import org.json4s.jackson.JsonMethods

object Json4sDemo {
  def main(args: Array[String]): Unit = {
    val s =
      """
        |{"name" : "lisi", "age" : 20}
        |""".stripMargin

    import org.json4s.JsonDSL._
    implicit val formats = DefaultFormats
    val value: JValue = JsonMethods.parse(s)
    val user = value.extract[User]
    println(user.toString)

  }

}
case class User(name : String, age : Int)