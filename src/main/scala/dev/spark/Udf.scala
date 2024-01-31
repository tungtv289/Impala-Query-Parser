package dev.spark

import dev.tungtv.{TableStatic, TrinoParser}
import org.apache.spark.sql.functions.udf

import java.util
import scala.collection.JavaConverters.asScalaSetConverter


case class TableStaticScala(cmd: String, dbName: String, tableName: String)


object Udf {

  def extractTable = udf((stmt: String) => if (stmt != null && stmt.nonEmpty) {
    val trinoParser: TrinoParser = new TrinoParser
    var rs: Set[TableStaticScala] = Set()
    try {
      val actual1: util.Set[TableStatic] = trinoParser.parser(stmt)
      for (tmp: TableStatic <- actual1.asScala.toSet) {
        rs = rs + TableStaticScala(tmp.cmd.toString, tmp.dbName, tmp.tableName)
      }
      Some(rs)
    } catch {
      case e: Exception => None
    }
  } else {
    None
  })

}
