package com.ligadata.kamanja.CheckerComponent;

import java.io.StringWriter;

import com.ligadata.kamanja.CheckerComponent.StringUtility;

public class ScalaHelper {
    String errorMessage = null;
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
            System.out.println("Scala started");
            StringBuffer output = new StringBuffer();
            status = "Success";
            String command = "which scala";
            /// root/Downloads/scala-2.10.4/bin/scala
            StringUtility str = new StringUtility();
            output = str.ExecuteSHCommandInputStream(command);
            String doc = str.replaceSpacesFromString(output.toString().trim().toLowerCase());
            int beginIndex = str.IndexOfString(doc, "scala-");
            // System.out.println(beginIndex);
            int lastIndex = str.IndexOfStringFrom(doc, beginIndex, "/");
            // System.out.println(lastIndex);
            // System.out.println(str.getWordBetweenIndex(doc, beginIndex +
            // "scala-".length(), lastIndex));
            version = str.getWordBetweenIndex(doc, beginIndex + "scala-".length(), lastIndex);
            System.out.println("Scala Successful");
            return version;
        } catch (Exception e) {
            // e.printStackTrace(new PrintWriter(errors));
            // errorMessage = errors.toString();
            status = "Fail";
            errorMessage = strutl.getStackTrace(e);
        }
        return null;
    }

    // public void ScalaVersionsh() {
    // try {
    // //System.out.println(System.getProperty("user.name"));
    // StringBuffer output = new StringBuffer();
    // //String command = "source .bashrc";
    // StringUtility str = new StringUtility();
    // //output = str.ExecuteSHCommandErrStream(command);
    // String command = "scala -version";
    // // StringUtility str = new StringUtility();
    // output = str.ExecuteSHCommandErrStream(command);
    // // System.out.println(output.toString());
    // String doc =
    // str.replaceSpacesFromString(output.toString().trim().toLowerCase());
    // int beginIndex = str.IndexOfString(doc, "version");
    // int lastIndex = str.IndexOfString(doc, "copyright");
    // // System.out.println(beginIndex);
    // // System.out.println(lastIndex);
    // String version = str.getWordBetweenIndex(doc, beginIndex + 7, lastIndex -
    // 2);
    // System.out.println(version); // scala version
    // } catch (Exception e) {
    // // e.printStackTrace(new PrintWriter(errors));
    // // errorMessage = errors.toString();
    // errorMessage = strutl.getStackTrace(e);
    // }
    //
    // }

    @SuppressWarnings("unused")
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
        version = ScalaVersion();
        // scala.ScalaVersionsh();
        if (version == null || version.trim().length() == 0 || errorMessage != null)
            status = "Fail";
    }

}
