package graph.sub_graph_hour

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object reformat_0 {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.select(
      col("cpd_dt"),
      concat(substring(col("cpd_tm"), 1, 2), lit(":00:00")).as("cpd_tm"),
      col("acct_num_regn_cd"),
      col("prod_brnd_grp_cd"),
      col("ntwrk_grp_cd"),
      col("cmls_tran_seq_id_drvd"),
      col("auth_aprv_cnt"),
      col("auth_tran_cnt"),
      col("auth_us_aprv_amt"),
      col("auth_us_tran_amt")
    )

}
