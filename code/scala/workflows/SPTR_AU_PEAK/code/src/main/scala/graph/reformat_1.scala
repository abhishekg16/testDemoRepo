package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object reformat_1 {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.select(
      col("cpd_dt"),
      when(length(col("cpd_tm")) === lit(1),
           concat(lit("00:00:0"), col("cpd_tm"))
      ).when(length(col("cpd_tm")) === lit(2),
              concat(lit("00:00:"), col("cpd_tm"))
        )
        .when(length(col("cpd_tm")) === lit(3),
              concat(lit("00:0"),
                     substring(col("cpd_tm"), 1, 1),
                     lit(":"),
                     substring(col("cpd_tm"), 2, 2)
              )
        )
        .when(length(col("cpd_tm")) === lit(4),
              concat(lit("00:"),
                     substring(col("cpd_tm"), 1, 2),
                     lit(":"),
                     substring(col("cpd_tm"), 3, 2)
              )
        )
        .when(
          length(col("cpd_tm")) === lit(5),
          concat(lit("0"),
                 substring(col("cpd_tm"), 1, 1),
                 lit(":"),
                 substring(col("cpd_tm"), 2, 2),
                 lit(":"),
                 substring(col("cpd_tm"), 4, 2)
          )
        )
        .otherwise(
          concat(substring(col("cpd_tm"), 1, 2),
                 lit(":"),
                 substring(col("cpd_tm"), 3, 2),
                 lit(":"),
                 substring(col("cpd_tm"), 5, 2)
          )
        )
        .as("cpd_tm"),
      col("acct_num_regn_cd"),
      col("prod_brnd_grp_cd"),
      col("ntwrk_grp_cd"),
      col("auth_aprv_cnt"),
      col("auth_tran_cnt"),
      col("auth_us_aprv_amt"),
      col("auth_us_tran_amt"),
      col("cmls_tran_seq_id_drvd")
    )

}
