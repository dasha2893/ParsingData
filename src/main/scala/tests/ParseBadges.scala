package tests

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by dasha on 23.04.15.
 */
object ParseBadges {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[4]").setAppName("test")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("D:\\stackexchange\\stackoverflow.com-PostHistory\\Badges.xml")
    rdd.map(line => {

      val map = scala.collection.mutable.Map[String,String]()

      val regex = """([A-z]*?)=(".*?")""".r
      for (p <- regex.findAllIn(line)) p match {
        case regex(key, value) => map += key -> value.replace("\"", "")
      }

      val id = map.getOrElse("Id","null")
      val userId = map.getOrElse("UserId","null")
      val name = map.getOrElse("Name","null")
      val date = if(map.getOrElse("Date","null").equals("null")) "null" else "'%s'".format(map.get("Date").get.replace("T", " ").replaceAll("\\.\\d{3}", ""))


      "INSERT INTO badges  VALUES (%s,%s,'%s',%s);".format(id, userId, name, date)
    }).
      saveAsTextFile("D:\\insert\\badges")
  }
}
