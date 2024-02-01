package dev.spark

import dev.tungtv.{ImpalaParser, TableStatic, TrinoParser}
import org.apache.spark.sql.functions.udf

import java.util
import scala.collection.JavaConverters.asScalaSetConverter


case class TableStaticScala(cmd: String, dbName: Option[String] = None, tableName: String)


object Udf {

  def extractTableTrino = udf((stmt: String) => if (stmt != null && stmt.nonEmpty) {
    val trinoParser: TrinoParser = new TrinoParser
    var rs: Set[TableStaticScala] = Set()
    try {
      val actual1: util.Set[TableStatic] = trinoParser.parser(stmt)
      for (tmp: TableStatic <- actual1.asScala.toSet) {
        var dbName: Option[String] = None
        if (tmp.dbName.nonEmpty) {
          dbName = Some(tmp.dbName)
        }
        rs = rs + TableStaticScala(tmp.cmd.toString, dbName, tmp.tableName)
      }
      Some(rs)
    } catch {
      case e: Exception => None
    }
  } else {
    None
  })

  def extractTableImpala = udf((stmt: String) => if (stmt != null && stmt.nonEmpty) {
    val impalaParser: ImpalaParser = new ImpalaParser
    var rs: Set[TableStaticScala] = Set()
    try {
      val actual1: util.Set[TableStatic] = impalaParser.parser(stmt)
      for (tmp: TableStatic <- actual1.asScala.toSet) {
        var dbName: Option[String] = None
        if (tmp.dbName != null && tmp.dbName.nonEmpty) {
          dbName = Some(tmp.dbName)
        }
        rs = rs + TableStaticScala(tmp.cmd.toString, dbName, tmp.tableName)
      }
      Some(rs)
    } catch {
      case e: Exception => None
    }
  } else {
    None
  })

}
