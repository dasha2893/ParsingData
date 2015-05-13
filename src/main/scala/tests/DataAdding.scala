package tests

import java.sql.{BatchUpdateException, DriverManager}
import scala.reflect.io.{Directory}

/**
 * Created by dasha on 11.05.2015.
 */
object DataAdding {
  def main(args: Array[String]) {

    val tableName = Array("users","posts","votes","posthistory","comments","badges","tages")
    for (table <- tableName) {
      addData(table)
    }

def addData(tableName: String) = {
      val connection = getConnection

      Directory("D:\\stackexchange\\" + tableName).files.foreach(file=>{
        file.lines.foreach(line=> {
          var statement = connection.createStatement()
          try {
            statement.executeUpdate(line)
          }catch {
            case e: Exception => println("exception caught: " + e);
          }
        })
        println("file = " + file)
      })
    }
  }

  def getConnection = {
    Class.forName("org.postgresql.Driver")
    val url = "jdbc:postgresql://localhost/stackoverflow"
    val user = "postgres"
    val password = "123"
    val connection = DriverManager.getConnection(url, user, password)
    connection
  }
}
