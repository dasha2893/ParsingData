package tests

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by dasha on 23.04.15.
 */
object ParsePosts {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","C:\\winutil")

    val conf = new SparkConf().setMaster("local[4]").setAppName("test")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("D:\\stackexchange\\stackoverflow.com-PostHistory\\Posts.xml")
    rdd.map(line => {

      val map = scala.collection.mutable.Map[String,String]()

      val regex = """([A-z]*?)=(".*?")""".r
      for (p <- regex.findAllIn(line)) p match {
        case regex(key, value) => map += key -> value.replace("\"", "")
      }

      val id = map.getOrElse("Id","null")
      val postTypeId = map.getOrElse("PostTypeId","null")
      val acceptedAnswerId = map.getOrElse("AcceptedAnswerId","null")
      val parentId = map.getOrElse("ParentId","null")
      val creationDate = if(map.getOrElse("CreationDate","null").equals("null")) "null" else "'%s'".format(map.get("CreationDate").get.replace("T", " ").replaceAll("\\.\\d{3}", ""))
      val score = map.getOrElse("Score","null")
      val viewCount = map.getOrElse("ViewCount","null")
      val body = map.getOrElse("Body","null").replace("'", "")
      val ownerUserId = map.getOrElse("OwnerUserId","null")
      val ownerDisplayName = map.getOrElse("OwnerDisplayName","null")
      val lastEditorUserId = map.getOrElse("LastEditorUserId","null")
      val lastEditorDisplayName = map.getOrElse("LastEditorDisplayName","null")
      val lastEditDate = if(map.getOrElse("LastEditDate","null").equals("null")) "null" else "'%s'".format(map.get("LastEditDate").get.replace("T", " ").replaceAll("\\.\\d{3}", ""))
      val lastActivityDate = if(map.getOrElse("LastActivityDate","null").equals("null")) "null" else "'%s'".format( map.get("LastActivityDate").get.replace("T", " ").replaceAll("\\.\\d{3}", ""))
      val title = map.getOrElse("Title","null").replace("'", "")
      val tags = map.getOrElse("Tags","null").replace("'", "")
      val answerCount = map.getOrElse("AnswerCount","null")
      val commentCount = map.getOrElse("CommentCount","null")
      val favoriteCount = map.getOrElse("FavoriteCount","null")
      val closedDate = if(map.getOrElse("ClosedDate","null").equals("null")) "null" else "'%s'".format(map.get("ClosedDate").get.replace("T", " ").replaceAll("\\.\\d{3}", ""))
      val communityOwnedDate = if(map.getOrElse("CommunityOwnedDate","null").equals("null")) "null" else "'%s'".format(map.get("CommunityOwnedDate").get.replace("T", " ").replaceAll("\\.\\d{3}", ""))


      "INSERT INTO posts VALUES (%s,%s,%s,%s,%s,%s,%s,'%s',%s,'%s',%s,'%s',%s,%s,'%s','%s',%s,%s,%s,%s,%s);".
        format(id, postTypeId, acceptedAnswerId, parentId, creationDate, score, viewCount, body, ownerUserId,
               ownerDisplayName, lastEditorUserId, lastEditorDisplayName, lastEditDate, lastActivityDate, title,
               tags, answerCount, commentCount, favoriteCount, closedDate, communityOwnedDate)

    }).
      saveAsTextFile("D:\\insert\\inserts/posts")

  }
}
