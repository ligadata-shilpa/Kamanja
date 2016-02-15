package check_prerequisites;

import java.io.PrintWriter;
import java.io.StringWriter;

public class JavaHelper {

	String component;
	String version;
	String nodeId;
	String status;
	String errorMessage;
	StringWriter errors = new StringWriter();

	private String CheckJavaVersion() {
		try {
			version = System.getProperty("java.version");
		} catch (Exception e) {
			e.printStackTrace(new PrintWriter(errors));
			errorMessage = errors.toString();
		}
		return version;
	}

	public void AskJava() {
		JavaHelper java = new JavaHelper();
		java.CheckJavaVersion();
	}

	public String getComponent() {
		return component;
	}

	public void setComponent(String component) {
		this.component = component;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public StringWriter getErrors() {
		return errors;
	}

	public void setErrors(StringWriter errors) {
		this.errors = errors;
	}
}
