package com.ligadata.KamanjaBase;

public abstract class ContainerFactoryInterface implements
		ContainerOrConceptFactory {

	enum ContainerType {
		MESSAGE, CONTAINER;
	}

	final public String getFullTypeName() {
		return (getTypeNameSpace() + "." + getTypeName());
	}

	final public boolean hasPrimaryKey() {
		String[] pKeys = getPrimaryKeyNames();
		return (pKeys != null && pKeys.length > 0);
	}

	final public boolean hasPartitionKey() {
		String[] pKeys = getPartitionKeyNames();
		return (pKeys != null && pKeys.length > 0);
	}

	final public boolean hasTimePartitionInfo() {
		TimePartitionInfo tmInfo = getTimePartitionInfo();
		return (tmInfo != null && tmInfo.timePartitionType != TimePartitionInfo.TimePartitionType.NONE);
	}

	public abstract String getTypeNameSpace();

	public abstract String getTypeName();

	public abstract String getTypeVersion();

	public abstract TimePartitionInfo getTimePartitionInfo();

	public abstract ContainerType getContainerType(); // ContainerType is enum of MESSAGE or CONTAINER

	public abstract boolean isFixed();

	public abstract String getSchema();

	public abstract String[] getPrimaryKeyNames();

	public abstract String[] getPartitionKeyNames();

	public abstract ContainerInterface createInstance();

}
