package graph.sub_graph_1

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
      col("cpd_tm"),
      col("acct_num_regn_cd"),
      lit("-").as("prod_brnd_grp_cd"),
      col("ntwrk_grp_cd"),
      col("auth_tran_cnt"),
      col("auth_aprv_cnt"),
      col("auth_us_aprv_amt"),
      col("auth_us_tran_amt"),
      lit("N").as("peak_rec_typ_cd")
    )

}
