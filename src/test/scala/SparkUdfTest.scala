import dev.spark.Udf.{extractTableImpala, extractTableTrino}
import org.apache.spark.sql.functions._
import org.junit.Test

class SparkUdfTest extends SparkCommon {
  @Test def udf_trino_parser(): Unit = {
    val filePath = Common.getAbsolutePathFromName("trino_audit_log.snappy.parquet")
    //    val df = _spark.read.parquet(filePath)
    val df = sparkSpec.read.parquet(filePath)
    val rs = df.select("QueryID", "Query", "QueryStartTime")
      .withColumnRenamed("QueryID", "query_id")
      .withColumn("TableStatic", extractTableTrino(col("Query")))
      .withColumn("table_static", explode(col("TableStatic")))
      .withColumn("data_date_key", date_format(to_timestamp(col("QueryStartTime")), "yyyyMMdd").cast("int"))
      .select("query_id", "table_static.*", "data_date_key")



    //      .show(false)
    //    val dbNull = rs
    //      .where("dbName is null")
    rs.show(false)
    df.printSchema()
  }

  @Test def udf_impala_parser(): Unit = {
    val filePath = Common.getAbsolutePathFromName("impala_audit_log.snappy.parquet")
    //    val df = _spark.read.parquet(filePath)
    //    val df = sparkSpec.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat").load(filePath)
    val df = sparkSpec.read.parquet(filePath)
    val rs = df.select("query_id", "statement", "start_time")
      .withColumnRenamed("query_id", "query_id")
      .withColumn("statement", regexp_replace(df("statement"), "\\r\\n", " "))
      .withColumn("TableStatic", extractTableImpala(col("statement")))
      .withColumn("table_static", explode(col("TableStatic")))
      .withColumn("data_date_key", date_format(to_timestamp(col("start_time")), "yyyyMMdd").cast("int"))
      .select("query_id", "table_static.*", "data_date_key")
    ////
    ////    //      .show(false)
    //    val dbNull = rs
    //      .where("dbName is not null")
    rs.show(false)
    //    rs.show(1, false)
    df.printSchema()
  }
}
