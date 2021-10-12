package config

import config.ConfigStore._
import io.prophecy.libs._

case class Config(
  fabricName:  String,
  CPD_DT:      String,
  RUN_DATE:    String,
  TARGET_PATH: String
) extends ConfigBase
