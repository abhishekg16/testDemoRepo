package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object filter_1 {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.filter(
      col("CMLS_REQST_MSG_TYP_DRVD")
        .isin("0100", "0200")
        .or(
          (col("CMLS_REQST_MSG_TYP_DRVD") === lit("0220"))
            .and(col("CMLS_POSCOND_CD") === lit("06"))
        )
        .and(col("CMLS_ISO_TRAN_AMT_USD") > lit(0))
        .and(
          lpad(col("CMLS_PROC_TRAN_CD"), 2,    "0")
            .isin("00",                  "01", "10", "11", "40", "50", "21")
            .and(
              (col("CMLS_CPD_TM") =!= lit(1)).or(col("CMLS_VIC") =!= lit("OCB"))
            )
        )
    )

}
