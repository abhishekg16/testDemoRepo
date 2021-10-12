package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object reformat_debit {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.select(
      col("cmls_acct_num"),
      col("cmls_cpd_dt"),
      col("cmls_auth_time").as("cmls_cpd_tm"),
      col("cmls_acct_regn_cd_drvd"),
      col("cmls_resp_cd"),
      col("cmls_iso_tran_amt_usd"),
      col("cmls_poscond_cd"),
      col("cmls_proc_tran_cd"),
      col("cmls_vic"),
      col("cmls_reqst_msg_typ_drvd"),
      col("cmls_ntwrk_id_enr"),
      col("cmls_acct_ctry_cd_drvd"),
      col("cmls_acct_rng_svc_cd_drvd"),
      col("cmls_acqr_bin_ctry_cd_drvd"),
      col("cmls_acqr_bin_regn_cd_drvd"),
      when((col("cmls_eci_moto_cd_ri") === lit("Y")).and(
             col("cmls_eci_moto_cd_drvd") === lit("00")
           ),
           lit("  ")
      ).when(col("cmls_eci_moto_cd_ri") === lit("Y"),
              col("cmls_eci_moto_cd_drvd")
        )
        .otherwise(lit("--"))
        .as("cmls_eci_moto_cd_drvd"),
      col("cmls_mrch_catg_cd_enr"),
      col("cmls_mrch_ctry_cd_num_drvd").as("cmls_mrch_ctry_cd_enr"),
      col("cmls_vcis_jrsdctn_cd_drvd"),
      col("cmls_mrch_regn_cd_enr"),
      when(trim(col("cmls_mrch_st_cd_glbl_enr")) === lit(""), lit("--"))
        .otherwise(col("cmls_mrch_st_cd_glbl_enr"))
        .as("cmls_mrch_st_cd_glbl_enr"),
      col("cmls_pos_env_cd_enr"),
      col("cmls_posentry_mode_cd_enr"),
      col("cmls_ppd_sub_typ_cd_drvd"),
      col("cmls_prod_typ_ext_drvd"),
      col("cmls_prod_num_alp_drvd"),
      col("cmls_prod_typ_cd_drvd"),
      when(col("cmls_ntwrk_id") === lit(3), lit("K"))
        .when(col("cmls_ntwrk_id").isin(4, 6, 7, 40, 41, 42), lit("P"))
        .when(col("cmls_ntwrk_id") === lit(50), lit("E"))
        .otherwise(lit("D"))
        .as("cmls_sys_srce_cd"),
      col("cmls_iso_tran_amt"),
      col("cmls_cshbk_amt_usd"),
      col("cmls_tran_seq_id_drvd"),
      col("cmls_acqr_ntwrk_id_drvd"),
      col("cmls_crd_id_mthd_cd"),
      col("cmls_acct_prod_id_alp_drvd"),
      col("cmls_prod_id_subtyp_cd_drvd"),
      col("cmls_acct_fundg_srce_cd_drvd"),
      col("cmls_acct_fundg_subtyp_cd_drvd"),
      col("cmls_acct_lvl_procg_cd_drvd"),
      col("cmls_prod_brnd_cd_drvd"),
      col("cmls_prod_id_num_drvd"),
      col("cmls_prod_num_drvd"),
      col("cmls_acct_num_ri")
    )

}
