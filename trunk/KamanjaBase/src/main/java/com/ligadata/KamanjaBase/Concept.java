package com.ligadata.KamanjaBase;

//FIXME:: Decide whether we need to generate concept class for every concept or use generic class like this
public class Concept implements ContainerOrConcept {
	String key = null;
	String valueType = null;
	Object value = null;

	Concept(String key, String valueType, Object value) {
		this.key = key;
		this.valueType = valueType;
		this.value = value;
	}
}