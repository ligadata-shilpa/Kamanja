/*
 * Copyright 2016 ligaDATA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
