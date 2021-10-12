package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object join_0 {

  def apply(
    spark: SparkSession,
    in0:   DataFrame,
    in1:   DataFrame,
    in2:   DataFrame
  ): DataFrame =
    in0
      .as("in0")
      .join(in1.as("in1"),
            col("in0.cmls_ntwrk_id_enr") === col("in1.ntwrk_id"),
            "left_outer"
      )
      .join(in2.as("in2"),
            (col("in0.cmls_resp_cd") === col("in2.auth_resp_cd"))
              .and(col("in2.dt") === lit(Config.RUN_DATE)),
            "inner"
      )
      .select(
        col("in0.cmls_acct_num").as("cmls_acct_num"),
        when((col("in0.cmls_prod_brnd_cd_drvd") === lit("VISA"))
               .or(col("in0.cmls_prod_brnd_cd_drvd") === lit("IL")),
             lit("V")
        ).when(col("in0.cmls_prod_brnd_cd_drvd") === lit("PLUS"), lit("P"))
          .otherwise(lit("O"))
          .as("prod_brnd_grp_cd"),
        coalesce(col("in1.ntwrk_grp_cd"),             lit("O")).as("ntwrk_grp_cd"),
        when(col("in2.auth_resp_rlup_cd") === lit(0), lit("1"))
          .otherwise(lit("0"))
          .as("auth_aprv_cnt"),
        lit(1).as("auth_tran_cnt"),
        when(col("in2.auth_resp_rlup_cd") === lit(0),
             col("in0.cmls_iso_tran_amt_usd")
        ).otherwise(lit(0)).as("auth_us_aprv_amt"),
        col("in0.cmls_tran_seq_id_drvd").as("cmls_tran_seq_id_drvd"),
        col("in0.cmls_cpd_dt").as("cpd_dt"),
        col("in0.cmls_acct_regn_cd_drvd").as("acct_num_regn_cd"),
        col("in0.cmls_cpd_tm").as("cpd_tm"),
        col("in0.cmls_iso_tran_amt_usd").as("auth_us_tran_amt"),
        col("in2.auth_resp_rlup_cd").as("auth_resp_rlup_cd")
      )

}
