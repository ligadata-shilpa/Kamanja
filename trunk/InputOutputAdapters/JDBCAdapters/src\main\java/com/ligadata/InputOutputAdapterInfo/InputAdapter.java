package com.ligadata.InputOutputAdapterInfo;

public interface InputAdapter {
	public  AdapterConfiguration inputConfig();  
	public  InputAdapterCallerContext callerCtxt();
	public  String UniqueName();
	public  String Category();
	public  void Shutdown();
	public  void StopProcessing();
	public  void StartProcessing(StartProcPartInfo partInfo[], boolean value);
	public  PartitionUniqueRecordKey[] GetAllPartitionUniqueRecordKey();
	public  PartitionUniqueRecordKey DeserializeKey(java.lang.String string);
	public  PartitionUniqueRecordValue DeserializeValue(java.lang.String string) ;
	public  scala.Tuple2<PartitionUniqueRecordKey, PartitionUniqueRecordValue>[] getAllPartitionBeginValues();
	public  scala.Tuple2<PartitionUniqueRecordKey, PartitionUniqueRecordValue>[] getAllPartitionEndValues();
}
