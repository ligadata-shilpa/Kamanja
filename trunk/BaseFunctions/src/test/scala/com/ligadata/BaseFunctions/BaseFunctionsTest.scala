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

package com.ligadata.BaseFunctions

import org.scalatest._

/**
  * Created by will on 2/26/16.
  */
class BaseFunctionsTest extends FlatSpec {

  "AndOp" should "take two parameters that are 'true' and return true" in {
    assert(AndOp.and(true, true))
  }

  it should "take the first parameter as false and the second parameter as true and return false" in {
    assert(!AndOp.and(false, true))
  }

  it should "take the first parameter as true and the second parameter as false and return false" in {
    assert(!AndOp.and(true, false))
  }

  it should "take the first parameter as false and the second parameter as false and return false" in {
    assert(!AndOp.and(false, false))
  }

  "OrOp" should "take two parameters that are 'true' and return true" in {
    assert(OrOp.or(true, true))
  }

  it should "take the first parameter as 'false' and the second parameter as 'true' and return 'true'" in {
    assert(OrOp.or(false, true))
  }

  it should "take the first parameter as 'true' and the second parameter as 'false' and return 'true'" in {
    assert(OrOp.or(true, false))
  }

  it shouldg "take the first parameter as 'false' and the second parameter as 'false' and return 'false'" in {
    assert(!OrOp.or(false, false))
  }
}
