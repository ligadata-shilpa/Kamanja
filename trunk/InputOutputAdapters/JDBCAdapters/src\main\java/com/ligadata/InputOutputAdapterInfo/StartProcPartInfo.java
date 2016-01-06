package com.ligadata.InputOutputAdapterInfo;

public abstract class StartProcPartInfo {
	private PartitionUniqueRecordKey _key;
	private PartitionUniqueRecordValue _val;
	private PartitionUniqueRecordValue _validateInfoVal;
	
	public abstract com.ligadata.InputOutputAdapterInfo.PartitionUniqueRecordKey _key();
	public abstract void _key_$eq(com.ligadata.InputOutputAdapterInfo.PartitionUniqueRecordKey key);
	public abstract com.ligadata.InputOutputAdapterInfo.PartitionUniqueRecordValue _val();
	public abstract void _val_$eq(com.ligadata.InputOutputAdapterInfo.PartitionUniqueRecordValue vaue);
	public abstract com.ligadata.InputOutputAdapterInfo.PartitionUniqueRecordValue _validateInfoVal();
	public abstract void _validateInfoVal_$eq(com.ligadata.InputOutputAdapterInfo.PartitionUniqueRecordValue value);
}	
