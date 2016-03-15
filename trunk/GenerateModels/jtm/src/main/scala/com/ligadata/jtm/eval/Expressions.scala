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
package com.ligadata.jtm.eval

/**
  *
  */
object Expressions {


  /** Find all logical column names that are encode in this expression $name
    *
    * $var
    * $ns.$var
    *
    * \$([a-zA-Z0-9_]+)
    * \$\{([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+)\}
    *
    * @param expression
    * @return
    */
  def ExtractColumnNames(expression: String): Set[String] = {

    // Extract single and multiple components names
    val regex1 = """\$([a-zA-Z0-9_]+)""".r
    val regex2 = """\$\{([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+)\}""".r
    val m1 = regex1.findAllMatchIn(expression).toArray
    val m2 = regex2.findAllMatchIn(expression).toArray
    m1.map(m => m.group(1)).toSet ++  m2.map(m => m.group(1)).toSet
  }
}
