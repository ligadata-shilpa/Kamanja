package org.kamanja;

public interface GenericMsgContainerObjBase {

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

	public abstract boolean hasPrimaryKey();

	public abstract boolean hasPartitionKey();

	public abstract boolean hasTimePartitionInfo();
	
	public abstract GenericMsgContainerBase createNewMsgContainer();
}
