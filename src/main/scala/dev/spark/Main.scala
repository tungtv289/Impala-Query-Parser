package dev.spark

import dev.spark.Udf.extractTableTrino
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}

object Main {
  def main(args: Array[String]): Unit = {
    val master = "local[*]"
    val appName = "Test UDF"

    val conf: SparkConf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    val _spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate()

    _spark.sparkContext.setLogLevel("ERROR")


    //    val df = _spark.read.parquet(filePath)
    val df = _spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .load("/home/ubuntu/IdeaProjects/Impala-Query-Parser/target/test-classes/trino_audit_log.snappy.parquet")
    val rs = df.select("QueryID", "Query")
      .withColumn("TableStatic", extractTableTrino(col("Query")))
      .withColumn("table_static", explode(col("TableStatic")))
      .select("QueryID", "table_static.*")

    //      .show(false)
    val count = rs
      .where("dbName is null")
      .show(false)
//    println(count)
//    rs.show(false)
    rs.printSchema()
  }
}
