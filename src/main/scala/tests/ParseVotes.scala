package tests

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by dasha on 23.04.15.
 */
object ParseVotes {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\winutil")

    val conf = new SparkConf().setMaster("local[4]").setAppName("test")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("D:\\stackexchange\\Votes.xml")
    rdd.map(line => {

      val map = scala.collection.mutable.Map[String,String]()

      val regex = """([A-z]*?)=(".*?")""".r
      for (p <- regex.findAllIn(line)) p match {
        case regex(key, value) => map += key -> value.replace("\"", "")
      }

      val id = map.getOrElse("Id","null")
      val postId = map.getOrElse("PostId","null")
      val voteTypeId = map.getOrElse("VoteTypeId","null")
      val userId = map.getOrElse("UserId","null")
      val creationDate = if(map.getOrElse("CreationDate","null").equals("null")) "null" else "'%s'".format(map.get("CreationDate").get.replace("T", " ").replaceAll("\\.\\d{3}", ""))
      val bountyAmount = map.getOrElse("BountyAmount","null")


      "INSERT INTO votes values (%s,%s,%s,%s,%s,%s);".format(id, postId, voteTypeId, userId, creationDate, bountyAmount)
    }).
      saveAsTextFile("D:\\stackexchange\\Votes")


  }
}
