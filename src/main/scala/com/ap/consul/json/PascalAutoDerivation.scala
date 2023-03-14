package com.ap.consul.json

import io.circe.generic.AutoDerivation
import io.circe.generic.extras.Configuration

trait PascalAutoDerivation extends AutoDerivation {

  protected implicit val config: Configuration =
    Configuration.default.withPascalCaseMemberNames

}
