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
package com.ligadata.models.samples.medical2.V1
import com.ligadata.KamanjaBase._
import com.ligadata.KvBase.TimeRange
import com.ligadata.kamanja.metadata.ModelDef
import com.ligadata.runtime.Log
import com.ligadata.runtime.Conversion
class TransactionIngestFactory(modelDef: ModelDef, nodeContext: NodeContext) extends ModelInstanceFactory(modelDef, nodeContext) {
  override def createModelInstance(): ModelInstance = return new TransactionIngest(this)
  override def getModelName: String = "com.ligadata.models.samples.medical2.TransactionIngest"
  override def getVersion: String = "0.0.1"
  override def createResultObject(): ModelResultBase = new MappedModelResults()
}
class TransactionIngest(factory: ModelInstanceFactory) extends ModelInstance(factory) {
  val conversion = new com.ligadata.runtime.Conversion
  val log = new com.ligadata.runtime.Log(this.getClass.getName)
  import log._
  override def execute(txnCtxt: TransactionContext, execMsgsSet: Array[ContainerOrConcept], triggerdSetIndex: Int, outputDefault: Boolean): Array[ContainerOrConcept] = {
    if (isTraceEnabled)
      Trace(s"Model::execute transid=%d triggeredset=%d outputdefault=%s".format(txnCtxt.transId, triggerdSetIndex, outputDefault.toString))
    if(isDebugEnabled)
    {
      execMsgsSet.foreach(m => Debug( s"Input: %s -> %s".format(m.getFullTypeName, m.toString())))
    }
    //
    //
    def exeGenerated_transactionmsg_1(msg1: com.ligadata.kamanja.samples.messages.V1000000.TransactionMsgIn): Array[MessageInterface] = {
      Debug("exeGenerated_transactionmsg_1")
      // Split the incoming data
      val arraydata: Array[String] = msg1.data.split(",")
      // extract the type
      val typeName: String = arraydata(0)
      def process_o1(): Array[MessageInterface] = {
        Debug("exeGenerated_transactionmsg_1::process_o1")
        if (!("com.ligadata.kamanja.samples.messages.Beneficiary" == typeName)) {
          Debug("Filtered: transactionmsg@o1")
          return Array.empty[MessageInterface]
        }
        val result = com.ligadata.kamanja.samples.messages.V1000000.Beneficiary.createInstance
        result.sp_copd = conversion.ToInteger(arraydata(17))
        result.benres_ip = conversion.ToDouble(arraydata(25))
        result.pppymt_op = conversion.ToDouble(arraydata(29))
        result.sp_osteoprs = conversion.ToInteger(arraydata(21))
        result.bene_county_cd = conversion.ToInteger(arraydata(8))
        result.bene_hi_cvrage_tot_mons = conversion.ToInteger(arraydata(9))
        result.bene_birth_dt = conversion.ToInteger(arraydata(2))
        result.sp_chrnkidn = conversion.ToInteger(arraydata(15))
        result.sp_diabetes = conversion.ToInteger(arraydata(19))
        result.desynpuf_id = arraydata(1)
        result.sp_depressn = conversion.ToInteger(arraydata(18))
        result.bene_smi_cvrage_tot_mons = conversion.ToInteger(arraydata(10))
        result.sp_chf = conversion.ToInteger(arraydata(14))
        result.sp_ra_oa = conversion.ToInteger(arraydata(22))
        result.medreimb_ip = conversion.ToDouble(arraydata(24))
        result.bene_esrd_ind = conversion.ToChar(arraydata(6))
        result.sp_ischmcht = conversion.ToInteger(arraydata(20))
        result.sp_cncr = conversion.ToInteger(arraydata(16))
        result.bene_sex_ident_cd = conversion.ToInteger(arraydata(4))
        result.pppymt_car = conversion.ToDouble(arraydata(32))
        result.sp_state_code = conversion.ToInteger(arraydata(7))
        result.plan_cvrg_mos_num = conversion.ToInteger(arraydata(12))
        result.bene_hmo_cvrage_tot_mons = conversion.ToInteger(arraydata(11))
        result.bene_race_cd = conversion.ToInteger(arraydata(5))
        result.benres_car = conversion.ToDouble(arraydata(31))
        result.pppymt_ip = conversion.ToDouble(arraydata(26))
        result.sp_alzhdmta = conversion.ToInteger(arraydata(13))
        result.medreimb_car = conversion.ToDouble(arraydata(30))
        result.bene_death_dt = conversion.ToInteger(arraydata(3))
        result.sp_strketia = conversion.ToInteger(arraydata(23))
        result.medreimb_op = conversion.ToDouble(arraydata(27))
        result.benres_op = conversion.ToDouble(arraydata(28))
        Array(result)
      }
      def process_o2(): Array[MessageInterface] = {
        Debug("exeGenerated_transactionmsg_1::process_o2")
        if (!("com.ligadata.kamanja.samples.messages.HL7" == typeName)) {
          Debug("Filtered: transactionmsg@o2")
          return Array.empty[MessageInterface]
        }
        val result = com.ligadata.kamanja.samples.messages.V1000000.HL71.createInstance
        result.set("desynpuf_id", arraydata(1))
        result.set("clm_id", arraydata(2))
        result.set("clm_from_dt", arraydata(3))
        result.set("clm_thru_dt", arraydata(4))
        result.set("bene_birth_dt", arraydata(5))
        result.set("bene_death_dt", arraydata(6))
        result.set("bene_sex_ident_cd", arraydata(7))
        result.set("bene_race_cd", arraydata(8))
        result.set("bene_esrd_ind", arraydata(9))
        result.set("sp_state_code", arraydata(10))
        result.set("bene_county_cd", arraydata(11))
        result.set("bene_hi_cvrage_tot_mons", arraydata(12))
        result.set("bene_smi_cvrage_tot_mons", arraydata(13))
        result.set("bene_hmo_cvrage_tot_mons", arraydata(14))
        result.set("plan_cvrg_mos_num", arraydata(15))
        result.set("sp_alzhdmta", arraydata(16))
        result.set("sp_chf", arraydata(17))
        result.set("sp_chrnkidn", arraydata(18))
        result.set("sp_cncr", arraydata(19))
        result.set("sp_copd", arraydata(20))
        result.set("sp_depressn", arraydata(21))
        result.set("sp_diabetes", arraydata(22))
        result.set("sp_ischmcht", arraydata(23))
        result.set("sp_osteoprs", arraydata(24))
        result.set("sp_ra_oa", arraydata(25))
        result.set("sp_strketia", arraydata(26))
        result.set("age", arraydata(27))
        result.set("infectious_parasitic_diseases", arraydata(28))
        result.set("neoplasms", arraydata(29))
        result.set("endocrine_nutritional_metabolic_diseases_immunity_disorders", arraydata(30))
        result.set("diseases_blood_blood_forming_organs", arraydata(31))
        result.set("mental_disorders", arraydata(32))
        result.set("diseases_nervous_system_sense_organs", arraydata(33))
        result.set("diseases_circulatory_system", arraydata(34))
        result.set("diseases_respiratory_system", arraydata(35))
        result.set("diseases_digestive_system", arraydata(36))
        result.set("diseases_genitourinary_system", arraydata(37))
        result.set("complications_of_pregnancy_childbirth_the_puerperium", arraydata(38))
        result.set("diseases_skin_subcutaneous_tissue", arraydata(39))
        result.set("diseases_musculoskeletal_system_connective_tissue", arraydata(40))
        result.set("congenital_anomalies", arraydata(41))
        result.set("certain_conditions_originating_in_the_perinatal_period", arraydata(42))
        result.set("symptoms_signs_ill_defined_conditions", arraydata(43))
        result.set("injury_poisoning", arraydata(44))
        result.set("factors_influencing_health_status_contact_with_health_services", arraydata(45))
        result.set("external_causes_of_injury_poisoning", arraydata(46))
        result.set("hypothyroidism", arraydata(47))
        result.set("infarction", arraydata(48))
        result.set("alzheimer", arraydata(49))
        result.set("alzheimer_related", arraydata(50))
        result.set("anemia", arraydata(51))
        result.set("asthma", arraydata(52))
        result.set("atrial_fibrillation", arraydata(53))
        result.set("hyperplasia", arraydata(54))
        result.set("cataract", arraydata(55))
        result.set("kidney_disease", arraydata(56))
        result.set("pulmonary_disease", arraydata(57))
        result.set("depression", arraydata(58))
        result.set("diabetes", arraydata(59))
        result.set("glaucoma", arraydata(60))
        result.set("heart_failure", arraydata(61))
        result.set("hip_pelvic_fracture", arraydata(62))
        result.set("hyperlipidemia", arraydata(63))
        result.set("hypertension", arraydata(64))
        result.set("ischemic_heart_disease", arraydata(65))
        result.set("osteoporosis", arraydata(66))
        result.set("ra_oa", arraydata(67))
        result.set("stroke", arraydata(68))
        result.set("breast_cancer", arraydata(69))
        result.set("colorectal_cancer", arraydata(70))
        result.set("prostate_cancer", arraydata(71))
        result.set("lung_cancer", arraydata(72))
        result.set("endometrial_cancer", arraydata(73))
        result.set("tobacco", arraydata(74))
        result.set("height", arraydata(75))
        result.set("weight", arraydata(76))
        result.set("systolic", arraydata(77))
        result.set("diastolic", arraydata(78))
        result.set("totalcholesterol", arraydata(79))
        result.set("ldl", arraydata(80))
        result.set("triglycerides", arraydata(81))
        result.set("shortnessofbreath", arraydata(82))
        result.set("chestpain", arraydata(83))
        result.set("aatdeficiency", arraydata(84))
        result.set("chroniccough", arraydata(85))
        result.set("chronicsputum", arraydata(86))
        Array(result)
      }
      def process_o3(): Array[MessageInterface] = {
        Debug("exeGenerated_transactionmsg_1::process_o3")
        if (!("com.ligadata.kamanja.samples.messages.inpatientclaim" == typeName)) {
          Debug("Filtered: transactionmsg@o3")
          return Array.empty[MessageInterface]
        }
        val result = com.ligadata.kamanja.samples.messages.V1000000.InpatientClaim.createInstance
        result.clm_pass_thru_per_diem_amt = conversion.ToDouble(arraydata(14))
        result.nch_bene_blood_ddctbl_lblty_am = conversion.ToDouble(arraydata(17))
        result.nch_bene_dschrg_dt = conversion.ToInteger(arraydata(19))
        result.nch_prmry_pyr_clm_pd_amt = conversion.ToDouble(arraydata(8))
        result.clm_pmt_amt = conversion.ToDouble(arraydata(7))
        result.desynpuf_id = arraydata(1)
        result.nch_bene_ip_ddctbl_amt = conversion.ToDouble(arraydata(15))
        result.icd9_prcdr_cds = conversion.ToIntegerArray(arraydata(22), "~")
        result.segment = conversion.ToInteger(arraydata(3))
        result.clm_admsn_dt = conversion.ToInteger(arraydata(12))
        result.clm_drg_cd = conversion.ToInteger(arraydata(20))
        result.icd9_dgns_cds = arraydata(21).split("~")
        result.prvdr_num = arraydata(6)
        result.clm_from_dt = conversion.ToInteger(arraydata(4))
        result.hcpcs_cds = conversion.ToIntegerArray(arraydata(23), "~")
        result.at_physn_npi = conversion.ToLong(arraydata(9))
        result.op_physn_npi = conversion.ToLong(arraydata(10))
        result.admtng_icd9_dgns_cd = arraydata(13)
        result.ot_physn_npi = conversion.ToLong(arraydata(11))
        result.clm_thru_dt = conversion.ToInteger(arraydata(5))
        result.clm_id = conversion.ToLong(arraydata(2))
        result.nch_bene_pta_coinsrnc_lblty_am = conversion.ToDouble(arraydata(16))
        result.clm_utlztn_day_cnt = conversion.ToInteger(arraydata(18))
        Array(result)
      }
      def process_o4(): Array[MessageInterface] = {
        Debug("exeGenerated_transactionmsg_1::process_o4")
        if (!("com.ligadata.kamanja.samples.messages.OutpatientClaim" == typeName)) {
          Debug("Filtered: transactionmsg@o4")
          return Array.empty[MessageInterface]
        }
        val result = com.ligadata.kamanja.samples.messages.V1000000.OutpatientClaim.createInstance
        result.nch_bene_blood_ddctbl_lblty_am = conversion.ToDouble(arraydata(12))
        result.nch_prmry_pyr_clm_pd_amt = conversion.ToDouble(arraydata(8))
        result.clm_pmt_amt = conversion.ToDouble(arraydata(7))
        result.desynpuf_id = arraydata(1)
        result.icd9_prcdr_cds = conversion.ToIntegerArray(arraydata(14), "~")
        result.segment = conversion.ToInteger(arraydata(3))
        result.icd9_dgns_cds = arraydata(13).split("~")
        result.prvdr_num = arraydata(6)
        result.clm_from_dt = conversion.ToInteger(arraydata(4))
        result.hcpcs_cds = conversion.ToIntegerArray(arraydata(18), "~")
        result.at_physn_npi = conversion.ToLong(arraydata(9))
        result.op_physn_npi = conversion.ToLong(arraydata(10))
        result.nch_bene_ptb_coinsrnc_amt = conversion.ToDouble(arraydata(16))
        result.admtng_icd9_dgns_cd = arraydata(17)
        result.nch_bene_ptb_ddctbl_amt = conversion.ToDouble(arraydata(15))
        result.ot_physn_npi = conversion.ToLong(arraydata(11))
        result.clm_thru_dt = conversion.ToInteger(arraydata(5))
        result.clm_id = conversion.ToLong(arraydata(2))
        Array(result)
      }
      process_o1()++
        process_o2()++
        process_o3()++
        process_o4()
    }
    // Evaluate messages
    val msgs = execMsgsSet.map(m => m.getFullTypeName -> m).toMap
    val msg1 = msgs.getOrElse("com.ligadata.kamanja.samples.messages.TransactionMsgIn", null).asInstanceOf[com.ligadata.kamanja.samples.messages.V1000000.TransactionMsgIn]
    // Main dependency -> execution check
    //
    val results: Array[MessageInterface] =
      (if(msg1!=null) {
        exeGenerated_transactionmsg_1(msg1)
      } else {
        Array.empty[MessageInterface]
      }) ++
        Array.empty[MessageInterface]
    if(isDebugEnabled)
    {
      results.foreach(m => Debug( s"Output: %s -> %s".format(m.getFullTypeName, m.toString())))
    }
    results.asInstanceOf[Array[ContainerOrConcept]]
  }
}
