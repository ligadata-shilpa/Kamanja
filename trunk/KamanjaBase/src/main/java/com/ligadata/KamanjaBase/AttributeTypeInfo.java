package com.ligadata.KamanjaBase;

public class AttributeTypeInfo {

	String name;
	int Index;
	TypeCategory typeCategory; // from TypeCategory
	byte valTypeId;
	byte keyTypeId;
	long valSchemaId;

	public AttributeTypeInfo(String name, int Index, TypeCategory typeCategory,
			byte valTypeId, byte keyTypeId, long valSchemaId) {
		this.name = name;
		this.Index = Index;
		this.typeCategory = typeCategory;
		this.valTypeId = valTypeId;
		this.valSchemaId = valSchemaId;
		this.keyTypeId = keyTypeId;
	}

	public enum TypeCategory {
		INT(0), STRING(1), FLOAT(2), DOUBLE(3), LONG(4), BYTE(5), CHAR(6), CONTAINER(
				1001), MAP(1002), ARRAY(1003);
		private int value;

		private TypeCategory(int value) {
			this.value = value;
		}

		public int getValue(){
			return this.value;
		}
	}

	public boolean IsContainer() {
		return (typeCategory == TypeCategory.CONTAINER);
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

	public TypeCategory getTypeCategary() {
		return (TypeCategory) typeCategory;
	}

	public void setTypeCategary(TypeCategory typeCategory) {
		this.typeCategory = typeCategory;
	}

	public byte getValTypeId() {
		return valTypeId;
	}

	public void setValTypeId(byte valTypeId) {
		this.valTypeId = valTypeId;
	}

	public byte getKeyTypeId() {
		return keyTypeId;
	}

	public void setKeyTypeId(byte keyTypeId) {
		this.keyTypeId = keyTypeId;
	}

	public long getValSchemaId() {
		return valSchemaId;
	}

	public void setValSchemaId(long valSchemaId) {
		this.valSchemaId = valSchemaId;
	}

}
