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

package com.ligadata.kamanja.samples.models;

import scala.Option;

import com.ligadata.KamanjaBase.*;
import com.ligadata.kamanja.metadata.ModelDef;

public class LowBalanceAlertModel extends ModelInstance {
	public LowBalanceAlertModel(ModelInstanceFactory factory) {
    	super(factory);
    }

    public ContainerOrConcept[] execute(TransactionContext txnCtxt, ContainerOrConcept[] execMsgsSet, int matchedInputSetIndex, boolean outputDefault) {
        if (execMsgsSet.length == 0) {
            ContainerInterface[] returnArr = new ContainerInterface[0];
            return returnArr;
        }
        //Get the current transaction data
        TransactionMsg rcntTxn = (TransactionMsg) execMsgsSet[0];

    	GlobalPreferences gPref = (GlobalPreferences) GlobalPreferences.getRecentOrNew(new String[]{"Type1"});  //(new String[]{"Type1"});

    	CustPreferences cPref = (CustPreferences) CustPreferences.getRecentOrNew();

    	if(cPref.minbalancealertoptout())
    	{
    		return null;
    	}

    	RddDate curDtTmInMs = RddDate.currentGmtDateTime();
    	CustAlertHistory alertHistory = (CustAlertHistory) CustAlertHistory.getRecentOrNew();

    	if(curDtTmInMs.timeDiffInHrs(new RddDate(alertHistory.alertdttminms())) < gPref.minalertdurationinhrs())
    	{
    		return null;
    	}


    	 if (rcntTxn.balance() >= gPref.minalertbalance())
    	      return null;

    	    long curTmInMs = curDtTmInMs.getDateTimeInMs();
    	    	    // create new alert history record and persist (if policy is to keep only one, this will replace existing one)
	    //CustAlertHistory.build().withalertdttminms(curTmInMs).withalerttype("lowbalancealert").Save();
	    CustAlertHistory ah = CustAlertHistory.build();
	    ah.set("alertdttminms",curTmInMs);
	    ah.set("alerttype","lowbalancealert");
	    ah.save();

		LowBalanceAlertOutputMsg msg = (LowBalanceAlertOutputMsg) LowBalanceAlertOutputMsg.createInstance();
		msg.set(0, rcntTxn.custid());
		msg.set(1, rcntTxn.branchid());
		msg.set(2, rcntTxn.accno());
		msg.set(3, rcntTxn.balance());
		msg.set(4, "lowBalanceAlert");
		msg.set(5, curTmInMs);
		ContainerInterface[] returnArr = new ContainerInterface[1];
		returnArr[0] = msg;
		return returnArr;
  }

    /**
     * @param inTxnContext
     */


    public boolean minBalanceAlertOptout(JavaRDD<CustPreferences> pref)
    {

    	return false;
    }

    public static class LowBalanceAlertFactory extends ModelInstanceFactory {
		public LowBalanceAlertFactory(ModelDef modelDef, NodeContext nodeContext) {
			super(modelDef, nodeContext);
		}
/*
		public boolean isValidMessage(MessageContainerBase msg) {
			return (msg instanceof TransactionMsg);
		}
*/
		public ModelInstance createModelInstance() {
			return new LowBalanceAlertModel(this);
		}

		public String getModelName() {
			return "LowBalanceAlert";
		}

		public String getVersion() {
			return "0.0.1";
		}

		public ModelResultBase createResultObject() {
			return new MappedModelResults();
		}
	}

}

