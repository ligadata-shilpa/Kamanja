package org.kamanja;

public abstract class GenericBaseMsg implements GenericMsgContainerBase{

	public boolean isMessage() {
		return true;
	}

	public boolean isContainer() {
		return false;
	}

}
