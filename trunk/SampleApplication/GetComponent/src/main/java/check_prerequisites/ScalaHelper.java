package check_prerequisites;


import java.io.StringWriter;

public class ScalaHelper {
	String errorMessage = null;
	String version = null;
	String status;
	public StringWriter getErrors() {
		return errors;
	}

	public void setErrors(StringWriter errors) {
		this.errors = errors;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	StringWriter errors = new StringWriter();
	public String ScalaVersion(){
//		try{
//		System.out.println(scala.util.Properties.versionString());
//		} catch(Exception e){
//			e.printStackTrace(new PrintWriter(errors));
//			errorMessage = errors.toString();
//		}
		return scala.util.Properties.versionString();
	}

	private void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public String getVersion() {
		return version;
	}

	private void setVersion(String version) {
		this.version = version;
	}

	public String getStatus() {
		return status;
	}

	private void setStatus(String status) {
		this.status = status;
	}

	public void AskScala() {
		ScalaHelper scala = new ScalaHelper();
		version = scala.ScalaVersion().substring(7);
	}

}
