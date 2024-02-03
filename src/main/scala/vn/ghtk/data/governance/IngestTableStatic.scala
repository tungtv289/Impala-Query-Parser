package vn.ghtk.data.governance

import dev.spark.Udf.{extractTableImpala, extractTableTrino}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object IngestTableStatic {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val spark = SparkSession.builder().config(sparkConf)
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    val df = spark.read.parquet("/user/hive/warehouse/ghtk.db/trino_audit_log")
    val rs = df.select("QueryID", "Query", "QueryStartTime")
      .withColumnRenamed("QueryID", "query_id")
      .withColumn("TableStatic", extractTableTrino(col("Query")))
      .withColumn("table_static", explode(col("TableStatic")))
      .withColumn("data_date_key", date_format(to_timestamp(col("QueryStartTime")), "yyyyMMdd").cast("int"))
      .select("query_id", "table_static.*", "data_date_key")

    //    val dbNull = rs
    //      .where("dbName is null")
    //    dbNull.show(false)
    rs.write
      .format("parquet")
      .mode("overwrite")
      .partitionBy("data_date_key")
      .parquet("/user/hive/warehouse/ghtk.db/table_statistics")


    //    val df = _spark.read.parquet(filePath)
    //    val df = sparkSpec.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat").load(filePath)
    val df1 = spark.read.parquet("/user/hive/warehouse/ghtk.db/impala_audit_log")
    val rs1 = df1.select("query_id", "statement", "start_time")
      .withColumnRenamed("query_id", "query_id")
      .withColumn("statement", regexp_replace(col("statement"), "\\r\\n", " "))
      .withColumn("TableStatic", extractTableImpala(col("statement")))
      .withColumn("table_static", explode(col("TableStatic")))
      .withColumn("data_date_key", date_format(to_timestamp(col("start_time")), "yyyyMMdd").cast("int"))
      .select("query_id", "table_static.*", "data_date_key")
    ////
    ////    //      .show(false)
    rs1.write
      .format("parquet")
      .mode("overwrite")
      .partitionBy("data_date_key")
      .parquet("/user/hive/warehouse/ghtk.db/table_statistics")
  }
}
