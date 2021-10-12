package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._

object source_1 {

  def apply(spark: SparkSession): DataFrame = {
    Config.fabricName match {
      case "testFabricAnk" =>
        spark.read
          .format("parquet")
          .schema(
            StructType(
              Array(
                StructField("ntwrk_id",             IntegerType,   true),
                StructField("usr_cd",               StringType,    true),
                StructField("appln_ntwrk_id",       IntegerType,   true),
                StructField("ntwrk_grp_cd",         StringType,    true),
                StructField("appln_ntwrk_grp_desc", StringType,    true),
                StructField("eff_start_dt",         DateType,      true),
                StructField("eff_upd_dt",           DateType,      true),
                StructField("eff_end_dt",           DateType,      true),
                StructField("actv_ind",             StringType,    true),
                StructField("version_num",          IntegerType,   true),
                StructField("insrt_ts",             TimestampType, true),
                StructField("upd_ts",               TimestampType, true),
                StructField("dt",                   IntegerType,   true)
              )
            )
          )
          .load(
            "dbfs:/FileStore/Users/MaciejV/ptr-data/TEDC_APPLN_NTWRK_GRP_small.parquet"
          )
      case _ =>
        throw new Exception("No valid dataset present to read fabric")
    }
  }

}
