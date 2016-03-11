package org.kamanja;

public abstract class GenericBaseContainer implements GenericMsgContainerBase {

	public boolean isMessage() {
		return false;
	}

	public boolean isContainer() {
		return true;
	}

}
