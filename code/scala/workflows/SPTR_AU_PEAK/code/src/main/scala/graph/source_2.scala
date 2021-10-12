package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._

object source_2 {

  def apply(spark: SparkSession): DataFrame = {
    Config.fabricName match {
      case "testFabricAnk" =>
        spark.read
          .format("avro")
          .schema(
            StructType(
              Array(
                StructField("auth_resp_cd",      StringType,    true),
                StructField("auth_resp_nm",      StringType,    true),
                StructField("auth_resp_rlup_cd", IntegerType,   true),
                StructField("auth_resp_rlup_nm", StringType,    true),
                StructField("eff_start_dt",      DateType,      true),
                StructField("eff_upd_dt",        DateType,      true),
                StructField("eff_end_dt",        DateType,      true),
                StructField("actv_ind",          StringType,    true),
                StructField("version_num",       IntegerType,   true),
                StructField("insrt_ts",          TimestampType, true),
                StructField("upd_ts",            TimestampType, true),
                StructField("dt",                IntegerType,   true)
              )
            )
          )
          .load(
            "dbfs:/FileStore/Users/MaciejV/ptr-data/TEDC_AUTH_RESP_CD.avro/"
          )
      case _ =>
        throw new Exception("No valid dataset present to read fabric")
    }
  }

}
