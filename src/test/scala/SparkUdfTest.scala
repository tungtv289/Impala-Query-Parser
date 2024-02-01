import dev.spark.Udf.{extractTableImpala, extractTableTrino}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, regexp_replace}
import org.junit.{Before, Test}

class SparkUdfTest {
  private val master = "local[*]"
  private val appName = "Test UDF"

  private var _spark: SparkSession = _

  def sparkSpec: SparkSession = _spark

  val conf: SparkConf = new SparkConf()
    .setMaster(master)
    .setAppName(appName)

  @Before def init(): Unit = {
    _spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate()

    _spark.sparkContext.setLogLevel("ERROR")

  }

  @Test def udf_trino_parser(): Unit = {
    val filePath = Common.getAbsolutePathFromName("trino_audit_log.snappy.parquet")
    //    val df = _spark.read.parquet(filePath)
    val df = sparkSpec.read.parquet(filePath)
    val rs = df.select("QueryID", "Query", "QueryStartTime")
      .withColumn("TableStatic", extractTableTrino(col("Query")))
      .withColumn("table_static", explode(col("TableStatic")))
      .select("QueryID", "table_static.*", "QueryStartTime")

    //      .show(false)
    val dbNull = rs
      .where("dbName is null")
    dbNull.show(false)
    df.printSchema()
  }

  @Test def udf_impala_parser(): Unit = {
    val filePath = Common.getAbsolutePathFromName("impala_audit_log.snappy.parquet")
    //    val df = _spark.read.parquet(filePath)
//    val df = sparkSpec.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat").load(filePath)
    val df = sparkSpec.read.parquet(filePath)
    val rs = df.select("query_id", "statement", "start_time")
      .withColumn("statement", regexp_replace(df("statement"), "\\r\\n", " "))
      .withColumn("TableStatic", extractTableImpala(col("statement")))
      .withColumn("table_static", explode(col("TableStatic")))
      .select("query_id", "table_static.*", "start_time")
////
////    //      .show(false)
    val dbNull = rs
      .where("dbName is not null")
    dbNull.show(false)
//    rs.show(1, false)
    df.printSchema()
  }
}
