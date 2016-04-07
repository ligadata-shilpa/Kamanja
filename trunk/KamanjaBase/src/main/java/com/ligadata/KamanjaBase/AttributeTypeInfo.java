package com.ligadata.KamanjaBase;

public class AttributeTypeInfo {

	String name;
	int Index;
	short typeCategary;
	short valTypeId;
	long valSchemaId;
	String keyTypeId;

	public AttributeTypeInfo(String name, int Index, short typeCategary, short valTypeId,
			long valSchemaId, String keyTypeId) {
		this.name = name;
		this.Index = Index;
		this.typeCategary = typeCategary;
		this.valTypeId = valTypeId;
		this.valSchemaId = valSchemaId;
		this.keyTypeId = keyTypeId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getIndex() {
		return Index;
	}

	public void setIndex(int index) {
		Index = index;
	}

	public short getTypeCategary() {
		return typeCategary;
	}

	public void setTypeCategary(short typeCategary) {
		this.typeCategary = typeCategary;
	}

	public short getValTypeId() {
		return valTypeId;
	}

	public void setValTypeId(short valTypeId) {
		this.valTypeId = valTypeId;
	}

	public long getValSchemaId() {
		return valSchemaId;
	}

	public void setValSchemaId(long valSchemaId) {
		this.valSchemaId = valSchemaId;
	}

	public String getKeyTypeId() {
		return keyTypeId;
	}

	public void setKeyTypeId(String keyTypeId) {
		this.keyTypeId = keyTypeId;
	}

}
