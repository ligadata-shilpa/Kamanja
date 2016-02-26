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

package com.ligadata.AuditAdapterInfo

import java.text.{SimpleDateFormat, DateFormat}
import java.util.Calendar

import org.json4s.JsonAST.JObject
import org.scalatest._

/**
  * Created by will on 2/26/16.
  */
class AuditRecordTest extends FlatSpec with BeforeAndAfter {

  private var auditRecord: AuditRecord = _

  before {
    auditRecord = new AuditRecord
  }

  "AuditRecord" should "display all variables in a comma-delimited format when toString is called" in {
    auditRecord.actionTime = "12:00"
    auditRecord.action = "Test"
    auditRecord.notes = "Just a little unit testing going on here"
    auditRecord.objectAccessed = "TestObject"
    auditRecord.success = "SuccessfullyTested"
    auditRecord.transactionId = "24"
    auditRecord.userOrRole = "SupremeLeader"
    auditRecord.userPrivilege = "SupremeAdmin"
    assert(auditRecord.toString == "(12:00,Test,TestObject,SuccessfullyTested,24,SupremeLeader,SupremeAdmin)")
  }

  it should "display all variables in a json format when toJson is called" in {
    val formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val calendar = Calendar.getInstance()
    val actionTime = calendar.getTimeInMillis
    val at = formatter.format(actionTime)

    auditRecord.actionTime = actionTime.toString
    auditRecord.action = "Test"
    auditRecord.notes = "Just a little unit testing going on here"
    auditRecord.objectAccessed = "TestObject"
    auditRecord.success = "SuccessfullyTested"
    auditRecord.transactionId = "24"
    auditRecord.userOrRole = "SupremeLeader"
    auditRecord.userPrivilege = "SupremeAdmin"

    val json: JObject = auditRecord.toJson
    assert((json \ "ActionTime").values == at)
    assert((json \ "Action").values == "Test")
    assert((json \ "ActionResult").values == "Just a little unit testing going on here")
    assert((json\ "ObjectAccessed").values == "TestObject")
    assert((json \ "UserOrRole").values == "SupremeLeader")
    assert((json \ "Status").values == "SuccessfullyTested")
  }
}