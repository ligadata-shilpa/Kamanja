package com.ligadata.KamanjaBase;

public abstract class ContainerInterface implements ContainerOrConcept {

    long transactionId = 0;
    long timePartitionData = 0;
    int rowNumber = 0;

    ContainerFactoryInterface factory = null;

    public ContainerInterface(ContainerFactoryInterface factory) {
        this.factory = factory;
    }

    final public boolean hasPrimaryKey() {
        return factory.hasPrimaryKey();
    }

    final public boolean hasPartitionKey() {
        return factory.hasPartitionKey();
    }

    final public boolean hasTimePartitionInfo() {
        return factory.hasTimePartitionInfo();
    }

    final public long getTransactionId() {
        return transactionId;
    }

    final public long getTimePartitionData() {
        return timePartitionData;
    }

    final public int getRowNumber() {
        return rowNumber;
    }

    final public String getFullTypeName() {
        return factory.getFullTypeName();
    }

    final public String getTypeNameSpace() {
        return factory.getTypeNameSpace();
    }

    final public String getTypeName() {
        return factory.getTypeName();
    }

    final public String getTypeVersion() {
        return factory.getTypeVersion();
    }

    final public ContainerFactoryInterface.ContainerType getContainerType() {
        return factory.getContainerType();
    }

    final public boolean isFixed() {
        return factory.isFixed();
    }

    final public String getSchema() {
        return factory.getSchema();
    }

    final public String[] getPrimaryKeyNames() {
        return factory.getPrimaryKeyNames();
    }

    final public String[] getPartitionKeyNames() {
        return factory.getPartitionKeyNames();
    }

    final public TimePartitionInfo getTimePartitionInfo() {
        return factory.getTimePartitionInfo();
    }

    final public ContainerFactoryInterface getFactory() {
        return this.factory;
    }

    final public void setTransactionId(long transId) {
        this.transactionId = transId;
    }

    final public void setTimePartitionData(long timePrtData) {
        this.timePartitionData = timePrtData;
    }

    final public void setRowNumber(int rowNbr) {
        this.rowNumber = rowNbr;
    }

    public abstract String[] getPrimaryKey();

    public abstract String[] getPartitionKey();

    public abstract void save();

    public abstract ContainerOrConcept Clone();

    public abstract String[] getAttributeNames();

    public abstract AttributeValue get(String key); // Return (value, type)

    public abstract AttributeValue get(int index); // Return (value, type)

    public abstract AttributeValue getOrElse(String key, Object defaultVal); // Return (value, type)

    public abstract AttributeValue getOrElse(int index, Object defaultVal); // Return (value,  type)

    public abstract java.util.HashMap<String, AttributeValue> getAllAttributeValues(); // Has (name, value, type))

    public abstract void set(String key, Object value);

    public abstract void set(int index, Object value);
    
    public abstract void set(String key, Object value, String valTyp) ;
    
    public abstract java.util.Iterator<java.util.Map.Entry<String, AttributeValue>> getAttributeNameAndValueIterator();
}
