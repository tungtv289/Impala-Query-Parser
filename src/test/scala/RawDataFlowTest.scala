import dev.spark.Udf.{extractTableImpala, extractTableTrino}
import org.apache.spark.sql.functions._
import org.junit.Test

class RawDataFlowTest extends SparkCommon {
  @Test def trino_flow(): Unit = {
    val filePath = Common.getAbsolutePathFromName("raw_trino.snappy.parquet")
    //    val df = _spark.read.parquet(filePath)
    val df = sparkSpec.read.parquet(filePath)

    df.createOrReplaceTempView("raw_data_bigdata_trino_audit_log")

    val sql =
      """
        |SELECT
        |	cast(`msg_tmp`.`QueryType` AS String) AS `QueryType`,
        |	cast(`msg_tmp`.`Query` AS String) AS `Query`,
        |	cast(`msg_tmp`.`User` AS String) AS `User`,
        |	cast(`msg_tmp`.`QueryID` AS String) AS `QueryID`,
        |	cast(`msg_tmp`.`QueryStartTime` AS String) AS `QueryStartTime`,
        |	cast(date_format(to_timestamp(`msg_tmp`.`QueryStartTime`), "yyyyMMdd") as int) as `data_date_key`
        |FROM (SELECT
        |from_json(msg, 'struct<QueryType String,Query String,User String,QueryID String,QueryStartTime String,QueryEndTime String,ExecutionStartTime String,CpuTime String,TotalBytes String,TotalRows String,OutputRows String,OutputBytes String,WrittenBytes String,WrittenRows String,PeakUserMemoryBytes String>') as msg_tmp
        |FROM raw_data_bigdata_trino_audit_log) tmp
        |""".stripMargin

    val trinoDf = sparkSpec.sql(sql)

    val rs = trinoDf
      .where("")
      .select("QueryID", "Query", "QueryStartTime")
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

}
