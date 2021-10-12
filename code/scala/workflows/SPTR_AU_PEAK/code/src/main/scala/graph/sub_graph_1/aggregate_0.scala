package graph.sub_graph_1

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object aggregate_0 {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.groupBy(
        col("cpd_dt"),
        col("cpd_tm"),
        col("acct_num_regn_cd"),
        col("prod_brnd_grp_cd"),
        col("ntwrk_grp_cd"),
        col("peak_rec_typ_cd")
      )
      .agg(
        sum(col("auth_tran_cnt")).as("auth_tran_cnt"),
        sum(col("auth_aprv_cnt")).as("auth_aprv_cnt"),
        sum(col("auth_us_aprv_amt"))
          .cast(DecimalType(18, 2))
          .as("auth_us_aprv_amt"),
        sum(col("auth_us_tran_amt"))
          .cast(DecimalType(18, 2))
          .as("auth_us_tran_amt")
      )

}
