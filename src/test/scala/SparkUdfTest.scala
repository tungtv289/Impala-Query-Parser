import dev.spark.Udf.extractTable
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
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

  @Test def whenCheckingListSize_thenSizeEqualsToInit(): Unit = {
    val filePath = Common.getAbsolutePathFromName("trino_audit_log.snappy.parquet")
    println(filePath)
//    val df = _spark.read.parquet(filePath)
    val df = _spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat").load(filePath)
    df.select("QueryID", "Query")
      .withColumn("TableStatic", extractTable(col("Query")))
      .show(1)
    df.printSchema()
  }
}
