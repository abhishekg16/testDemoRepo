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
    val df_source_0_out        = source_0(spark)
    val df_filter_credit_out   = filter_credit(spark,   df_source_0_out)
    val df_filter_debit_out    = filter_debit(spark,    df_source_0_out)
    val df_reformat_debit_out  = reformat_debit(spark,  df_filter_debit_out)
    val df_reformat_credit_out = reformat_credit(spark, df_filter_credit_out)
    val df_set_operation_0_out =
      set_operation_0(spark, df_reformat_debit_out, df_reformat_credit_out)
    val df_filter_1_out = filter_1(spark, df_set_operation_0_out)
    val df_source_1_out = source_1(spark)
    val df_filter_2_out = filter_2(spark, df_source_1_out)
    val df_source_2_out = source_2(spark)
    val df_join_0_out =
      join_0(spark, df_filter_1_out, df_filter_2_out, df_source_2_out)
    val df_reformat_1_out  = reformat_1(spark,  df_join_0_out)
    val df_aggregate_0_out = aggregate_0(spark, df_reformat_1_out)
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
