package tests

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by dasha on 23.04.15.
 */
object ParsePostLinks {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[4]").setAppName("test")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("D:\\stackexchange\\stackoverflow.com-PostHistory\\PostLinks.xml")
    rdd.map(line => {

      val map = scala.collection.mutable.Map[String,String]()

      val regex = """([A-z]*?)=(".*?")""".r
      for (p <- regex.findAllIn(line)) p match {
        case regex(key, value) => map += key -> value.replace("\"", "")
      }

      val id = map.getOrElse("Id","null")
      val creationDate = if(map.getOrElse("CreationDate","null").equals("null")) "null" else "'%s'".format(map.get("CreationDate").get.replace("T", " ").replaceAll("\\.\\d{3}", ""))
      val postId = map.getOrElse("PostId","null")
      val relatedPostId = map.getOrElse("RelatedPostId","null")
      val linkTypeId = map.getOrElse("LinkTypeId","null")


      "INSERT INTO postLinks  VALUES (%s,%s,%s,%s,%s);".format(id, creationDate, postId, relatedPostId, linkTypeId)

    }).
      saveAsTextFile("D:\\insert\\postlinks")
  }
}
