package org.kamanja;

public abstract class GenericBaseContainerObj implements GenericMsgContainerObjBase{

	public boolean isMessage() {
		return false;
	}

	public boolean isContainer() {
		return true;
	}

}
