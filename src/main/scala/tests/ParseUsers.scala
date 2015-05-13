package tests

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by dasha on 22.04.15.
 */
object ParseUsers {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\winutil")

    val conf = new SparkConf().setMaster("local[4]").setAppName("test")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("D:\\stackexchange\\Users.xml")
    rdd.map(line => {

      val map = scala.collection.mutable.Map[String,String]()

      val regex = """([A-z]*?)=(".*?")""".r
      for (p <- regex.findAllIn(line)) p match {
        case regex(key, value) => map += key -> value.replace("\"", "")
      }

      val id = map.getOrElse("Id","null")
      val reputation = map.getOrElse("Reputation","null")
      val creationDate = if(map.getOrElse("CreationDate","null").equals("null")) "null" else "'%s'".format(map.get("CreationDate").get.replace("T", " ").replaceAll("\\.\\d{3}", ""))
      val displayName = map.getOrElse("DisplayName","null")
      val lastAccessDate = if(map.getOrElse("LastAccessDate","null").equals("null")) "null" else "'%s'".format(map.get("LastAccessDate").get.replace("T", " ").replaceAll("\\.\\d{3}", ""))
      val websiteUrl = map.getOrElse("WebsiteUrl","null")
      val location = map.getOrElse("Location","null")
      val aboutMe = map.getOrElse("AboutMe","null").replace("'", "")
      val views = map.getOrElse("Views","null")
      val upVotes = map.getOrElse("UpVotes","null")
      val downVotes = map.getOrElse("DownVotes","null")
      val profileImageUrl = map.getOrElse("ProfileImageUrl","null")
      val emailHash = map.getOrElse("EmailHash","null")
      val age = map.getOrElse("Age","null")
      val accountId = map.getOrElse("AccountId","null")


      "INSERT INTO users values (%s,%s,%s,'%s',%s,'%s','%s','%s',%s,%s,%s,'%s','%s',%s,%s);".format(id, reputation, creationDate, displayName, lastAccessDate, websiteUrl, location, aboutMe, views, upVotes, downVotes, profileImageUrl, emailHash, age, accountId)
    }).
      saveAsTextFile("D:\\insert into stackoverflow\\Users")
  }
}
