package tests

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by dasha on 23.04.15.
 */
object ParseTags {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[4]").setAppName("test")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("D:\\stackexchange\\stackoverflow.com-PostHistory\\Tags.xml")
    rdd.map(line => {


      val map = scala.collection.mutable.Map[String,String]()

      val regex = """([A-z]*?)=(".*?")""".r
      for (p <- regex.findAllIn(line)) p match {
        case regex(key, value) => map += key -> value.replace("\"", "")
      }


      val id = map.getOrElse("Id","null")
      val tagName = map.getOrElse("TagName","null")
      val count = map.getOrElse("Count","null")
      val wikiPostId = map.getOrElse("WikiPostId","null")


      "INSERT INTO tags  VALUES (%s,'%s',%s,%s);".format(id, tagName, count, wikiPostId)
    }).
      saveAsTextFile("D:\\insert\\inserts/tags")


  }
}
