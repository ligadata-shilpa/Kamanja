package com.ligadata.dataGenerationTool.fields;


public class CopsFields {

	public enum EventSource {
		EventSource1, EventSource2, EventSource3, EventSource4;
	}

	public enum authenticationMethod {
		certificate, LDAP, federation
	}

	public enum action {
		login, logout, concurrentlogin, accountlock, accountunlock, lockout, impersonate, permissionchanges, adminaccess, passwordreset, create, read, update, delete, download, copy
	}

	public enum resourceType {
		desktopapp, webapp, webservice, middleware, messaging, database, file, script, account
	}

	public enum confidentialDataLabels {
		ssntin, pinpassword, creditcardnumber, debitcardnumber, mortgageloannumber, helocnumber, cdnumber, insurancepolicynumber, otheraccountnumber, firstname, lastname, addressline1, addressline2, city, state, zip, email, phone, otheridentification, challengeresponseforforgottenpassword, licensenumber, passportnumber, taxid, nationalid
	}

	public enum resourceProtocol {
		TCP, IP, UDP, POP, SMTP, HTTP, FTP
	}

	public enum userRole {
		manager, employee, HR
	}

	@SuppressWarnings("unchecked")
	public <T extends Enum<T>> T EnumLookup(String enumName) {

		switch (enumName.toLowerCase()) {
		case "authenticationmethod":
			return (T) getValuesFromEnum(authenticationMethod.class);
		case "eventsource":
			return (T) getValuesFromEnum(EventSource.class);
		case "action":
			return (T) getValuesFromEnum(resourceType.class);
		case "confidentialdatalabels":
			return (T) getValuesFromEnum(confidentialDataLabels.class);
		case "resourceprotocol":
			return (T) getValuesFromEnum(resourceProtocol.class);
		case "userrole":
			return (T) getValuesFromEnum(userRole.class);
		default:
			return null;
		}

	}

	public <T extends Enum<T>> T getValuesFromEnum(Class<T> enumClass) {
		T[] valuesArray = enumClass.getEnumConstants();
		return valuesArray[(int) (Math.random() * valuesArray.length)];

	}

}
