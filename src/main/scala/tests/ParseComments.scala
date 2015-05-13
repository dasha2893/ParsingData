package tests

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by dasha on 23.04.15.
 */
object ParseComments {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[4]").setAppName("test")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("D:\\stackexchange\\stackoverflow.com-PostHistory\\Comments.xml")
    rdd.map(line => {

      val map = scala.collection.mutable.Map[String,String]()

      val regex = """([A-z]*?)=(".*?")""".r
      for (p <- regex.findAllIn(line)) p match {
        case regex(key, value) => map += key -> value.replace("\"", "")
      }

      val id = map.getOrElse("Id","null")
      val postId = map.getOrElse("PostId","null")
      val score = map.getOrElse("Score","null")
      val text = map.getOrElse("Text","null").replace("'", "")
      val creationDate = if(map.getOrElse("CreationDate","null").equals("null")) "null" else "'%s'".format(map.get("CreationDate").get.replace("T", " ").replaceAll("\\.\\d{3}", ""))
      val userDisplayName = map.getOrElse("UserDisplayName","null")
      val userId = map.getOrElse("UserId","null")


      "INSERT INTO comments VALUES (%s,%s,%s,'%s',%s,'%s',%s);".format(id, postId, score, text, creationDate, userDisplayName, userId)
    }).
      saveAsTextFile("D:\\insert\\comments")
  }
}
