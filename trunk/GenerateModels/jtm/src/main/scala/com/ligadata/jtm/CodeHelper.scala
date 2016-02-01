package com.ligadata.jtm

/**
  * Created by joerg on 1/31/16.
  */
object CodeHelper {

  def Indent(source : String) : String = {

    val sb = new StringBuilder
    val lines = source.split('\n')
    var open = 0
    var empty = 0

    lines.foreach( l => {

      val l1 = l.trim

      if(l1.length==0) {
        if(empty==0) {
          //sb.append("\n")
        }
        empty = empty + 1
      } else {
        empty = 0

        if(l1.startsWith("}")) {
          open = open - 1
        }

        for(i <- 1 to open) {
          sb.append("  ")
        }

        sb.append(l1)
        sb.append("\n")

        if(l1.endsWith("{")) {
            open = open + 1
        }
      }
    })

    sb.toString
  }
}
