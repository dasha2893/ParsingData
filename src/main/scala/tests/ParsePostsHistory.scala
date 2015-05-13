package tests

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by dasha on 24.04.15.
 */
object ParsePostsHistory {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\winutil")

    val conf = new SparkConf().setMaster("local[4]").setAppName("test")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("D:\\stackexchange\\stackoverflow.com-PostHistory\\PostHistory.xml")
    rdd.map(line => {

      val map = scala.collection.mutable.Map[String,String]()

      val regex = """([A-z]*?)=(".*?")""".r
      for (p <- regex.findAllIn(line)) p match {
        case regex(key, value) => map += key -> value.replace("\"", "")
      }

      val id = map.getOrElse("Id","null")
      val postHistoryTypeId = map.getOrElse("PostHistoryTypeId","null")
      val postId = map.getOrElse("PostId","null")
      val revisionGuid = map.getOrElse("RevisionGuid", "null")
      val creationDate = if(map.getOrElse("CreationDate","null").equals("null")) "null" else "'%s'".format(map.get("CreationDate").get.replace("T", " ").replaceAll("\\.\\d{3}", ""))
      val userId = map.getOrElse("UserId","null")
      val userDisplayName = map.getOrElse("UserDisplayName","null")
      val comment = map.getOrElse("Comment","null").replace("'", "")
      val text = map.getOrElse("Text","null").replace("'", "")


      "INSERT INTO postHistory  VALUES (%s,%s,%s,%s,%s,%s,'%s','%s','%s');".
        format(id, postHistoryTypeId, postId, revisionGuid, creationDate, userId, userDisplayName, comment, text)

    }).take(500).foreach(line => println(line))
//      saveAsTextFile("D:\\insert\\posthistory")

  }
}
