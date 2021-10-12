package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object filter_debit {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.filter(
      (col("cpd_dt") === lit(Config.CPD_DT))
        .and(substring(col("cmls_tran_seq_id_drvd"), 4, 1) === lit("D"))
        .and(
          col("CMLS_REQST_MSG_TYP").isin("0100",
                                         "0200",
                                         "0201",
                                         "0220",
                                         "0221",
                                         "0400",
                                         "0420",
                                         "0421",
                                         "0422",
                                         "0423",
                                         "9240",
                                         "9250",
                                         "9242",
                                         "9252"
          )
        )
        .and(
          !col("cmls_acqr_pcr_drvd")
            .isin("9088", "8088")
            .and(
              !col("cmls_vip_rsi_msg_catgy")
                .isin("00155", "00156", "00159", "00094")
            )
        )
    )

}
