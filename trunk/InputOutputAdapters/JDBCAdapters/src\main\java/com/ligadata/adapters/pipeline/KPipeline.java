package com.ligadata.adapters.pipeline;
import com.ligadata.InputOutputAdapterInfo.AdapterConfiguration;
import com.ligadata.InputOutputAdapterInfo.InputAdapter;
import com.ligadata.InputOutputAdapterInfo.InputAdapterCallerContext;
import com.ligadata.InputOutputAdapterInfo.PartitionUniqueRecordKey;
import com.ligadata.InputOutputAdapterInfo.PartitionUniqueRecordValue;
import com.ligadata.InputOutputAdapterInfo.StartProcPartInfo;

import scala.Tuple2;

public class KPipeline implements InputAdapter {
	
	IPipeline pipeline;

	@Override
	public AdapterConfiguration inputConfig() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputAdapterCallerContext callerCtxt() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String UniqueName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String Category() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void Shutdown() {
		// TODO Auto-generated method stub

	}

	@Override
	public void StopProcessing() {
		// TODO Auto-generated method stub

	}

	@Override
	public void StartProcessing(StartProcPartInfo[] partInfo, boolean value) {
		// TODO Auto-generated method stub

	}

	@Override
	public PartitionUniqueRecordKey[] GetAllPartitionUniqueRecordKey() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PartitionUniqueRecordKey DeserializeKey(String string) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PartitionUniqueRecordValue DeserializeValue(String string) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Tuple2<PartitionUniqueRecordKey, PartitionUniqueRecordValue>[] getAllPartitionBeginValues() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Tuple2<PartitionUniqueRecordKey, PartitionUniqueRecordValue>[] getAllPartitionEndValues() {
		// TODO Auto-generated method stub
		return null;
	}

}
