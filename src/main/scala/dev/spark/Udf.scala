package dev.spark

import dev.tungtv.{ImpalaParser, QueryParser, TableStatic, TrinoParser}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.util
import scala.collection.JavaConverters.asScalaSetConverter

case class TableStaticScala(cmd: String, dbName: Option[String], tableName: Option[String])

object Udf {
  def parser(parser: QueryParser, stmt: String): Option[Set[TableStaticScala]] = {
    if (stmt != null && stmt.nonEmpty) {
      var rs: Set[TableStaticScala] = Set()
      try {
        val actual1: util.Set[TableStatic] = parser.parser(stmt)
        for (tmp: TableStatic <- actual1.asScala.toSet) {
          rs = rs + TableStaticScala(tmp.cmd.toString, Option(tmp.dbName), Option(tmp.tableName))
        }
        Some(rs)
      } catch {
        case e: Exception => None
      }
    } else {
      None
    }
  }

  def extractTableTrino: UserDefinedFunction = udf((stmt: String) => parser(new TrinoParser, stmt))

  def extractTableImpala: UserDefinedFunction = udf((stmt: String) => parser(new ImpalaParser, stmt))

}
