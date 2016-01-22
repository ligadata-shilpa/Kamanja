/*
 * Copyright 2015 ligaDATA
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
package com.ligadata.jtm.test

import java.io.File

import com.ligadata.jtm._
import org.apache.commons.io.FileUtils

import org.scalatest.{BeforeAndAfter, FunSuite}
/**
  *
  */
class FilterTest  extends FunSuite with BeforeAndAfter {

  test("nonaggr.jtm/filter.jtm") {

    // nonaggr.jtm/filter.jtm -> filter.scala
    //
    val fileInput = getClass.getResource("/nonaggr.jtm/filter.jtm").getPath
    val fileOutput = getClass.getResource("/nonaggr.jtm/filter.scala.result").getPath
    val fileExpected = getClass.getResource("/nonaggr.jtm/filter.scala.expected").getPath

    val compiler = CompilerBuilder.create().
      setSuppressTimestamps().
      setInputFile(fileInput).
      setOutputFile(fileOutput).
      build()

    val outputFile = compiler.Execute()

    val expected = "test result" // FileUtils.readFileToString(new File(fileExpected), null)
    val actual = FileUtils.readFileToString(new File(outputFile), null)

    assert(actual == expected)
  }
}
