package com.ligadata.kamanja.get_component;

import java.io.StringWriter;

public class ScalaHelper {
	static String errorMessage = null;
	String version = null;
	String status;
	StringWriter errors = new StringWriter();
	StringUtility strutl = new StringUtility();

	public StringWriter getErrors() {
		return errors;
	}

	public void setErrors(StringWriter errors) {
		this.errors = errors;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public String ScalaVersion() {
		try {
			return scala.util.Properties.versionString();
		} catch (Exception e) {
			// e.printStackTrace(new PrintWriter(errors));
			// errorMessage = errors.toString();
			errorMessage = strutl.getStackTrace(e);
		}
		return null;
	}

	@SuppressWarnings({ "static-access", "unused" })
	private void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public String getVersion() {
		return version;
	}

	@SuppressWarnings("unused")
	private void setVersion(String version) {
		this.version = version;
	}

	public String getStatus() {
		return status;
	}

	@SuppressWarnings("unused")
	private void setStatus(String status) {
		this.status = status;
	}

	public void AskScala() {
		ScalaHelper scala = new ScalaHelper();
		version = scala.ScalaVersion().substring(7);
	}

}
