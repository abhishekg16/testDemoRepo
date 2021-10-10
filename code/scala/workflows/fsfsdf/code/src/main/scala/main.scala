import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._
import graph._

object Main {

  def apply(spark: SparkSession): Unit = {
    val df_source_0_out0    = source_0(spark)
    val df_reformat_0_out_0 = reformat_0(spark)
    val df_set_operation_0_out_0 =
      set_operation_0(spark, df_source_0_out0, df_source_0_out0)
    df_reformat_0_out_0.cache().count()
    df_reformat_0_out_0.unpersist()
    df_set_operation_0_out_0.cache().count()
    df_set_operation_0_out_0.unpersist()
  }

  def main(args: Array[String]): Unit = {
    import config._
    ConfigStore.Config = ConfigurationFactoryImpl.fromCLI(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Workflow")
      .config("spark.default.parallelism", "4")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setCheckpointDir("/tmp/checkpoints")
    apply(spark)
  }

}
