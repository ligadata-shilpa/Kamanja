package org.kamanja;

public abstract class GenericMsgBaseMsgObj implements GenericMsgContainerObjBase{

	public boolean isMessage() {
		return true;
	}

	public boolean isContainer() {
		return false;
	}

}
