import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.Before

trait SparkCommon {
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

}
