package config

import config.ConfigStore._
import io.prophecy.libs._

case class Config(fabricName: String, RUN_DATE: String, TARGET_PATH: String)
    extends ConfigBase
