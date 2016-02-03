package com.ligadata.jtm.nodes

import com.google.gson.annotations.SerializedName

/**
  * Created by joerg on 1/26/16.
  */
class Compute {
  @SerializedName("type")
  val typename: String = ""
  @SerializedName("val")
  val expression: String = ""
  val comment: String = ""
}
