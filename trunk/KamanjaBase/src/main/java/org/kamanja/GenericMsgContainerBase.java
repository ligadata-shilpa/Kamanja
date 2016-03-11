package org.kamanja;

public interface GenericMsgContainerBase {

	long transactionId = 0;
	long timePartitionData = 0;
	int rowNumber = 0;

	/** System Columns Start ***/
	public abstract void setTransactionId(long transId);

	public abstract long getTransactionId();

	public abstract void setTimePartitionData(long timePrtData);

	public abstract long getTimePartitionData();

	public abstract void setRowNumber(long rowNbr);

	public abstract long getRowNumber();

	/** System Columns End ***/
	public abstract String getFullName();

	public abstract String getNameSpace();

	public abstract String getName();

	public abstract String getVersion();

	public abstract boolean isMessage();

	public abstract boolean isContainer();

	public abstract boolean isFixed();

	public abstract String getSchema();

	public abstract String[] getPrimaryKeys();

	public abstract String[] getPartitionKeys();

	public abstract String[] getPrimaryKeyData();

	public abstract String[] getPartitionKeyData();
	
	public abstract TimePartitionInfo getTimePartitionInfo();

	public abstract void Save();

	public abstract GenericMsgContainerBase Clone();

	public abstract boolean hasPrimaryKey();

	public abstract boolean hasPartitionKey();

	public abstract boolean hasTimePartitionInfo();

	public abstract String[] getKeys();

	public abstract ValuePair get(String key);

	public abstract ValuePair getOrElse(String key, Object defaultVal);

	public abstract String getKeysByOffset(int offset);

	public abstract KeyValuePair[] getAllKeysAndValues();

	public abstract void set(String key, Object value);

	public abstract void setKeysByOffset(int offset, ValuePair value);

}
