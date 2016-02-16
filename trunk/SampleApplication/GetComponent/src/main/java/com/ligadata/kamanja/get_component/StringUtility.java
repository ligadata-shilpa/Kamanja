package com.ligadata.kamanja.get_component;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;

public class StringUtility {
	public StringBuffer ExecuteSHCommandErrStream(String command) {
		StringBuffer output = new StringBuffer();
		Process p;
		try {
			p = Runtime.getRuntime().exec(new String[] { "sh", "-c", command });
			p.waitFor();
			BufferedReader reader = new BufferedReader(new InputStreamReader(p.getErrorStream()));
			String line = "";
			while ((line = reader.readLine()) != null) {
				output.append(line + "\n");
			}
			// System.out.println(output.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return output;
	}

	public StringBuffer ExecuteSHCommandInputStream(String command) {
		StringBuffer output = new StringBuffer();
		Process p;
		try {
			p = Runtime.getRuntime().exec(new String[] { "sh", "-c", command });
			p.waitFor();
			BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line = "";
			while ((line = reader.readLine()) != null) {
				output.append(line + "\n");
			}
			// System.out.println(output.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return output;
	}

	public int IndexOfString(String doc, String word) {
		return doc.indexOf(word);
	}

	public int IndexOfStringFrom(String doc, int beginIndex,String word) {
		return doc.indexOf(word, beginIndex);
	}
	
	public String replaceSpacesFromString(String doc) {
		return doc.replaceAll("\\s", "");
	}

	public String getWordBetweenIndex(String doc, int beginIndex, int lastIndex) {
		return doc.trim().toLowerCase().substring(beginIndex, lastIndex);
	}

	public int IndexFrom(String doc, int beginIndex, String word) {
		return doc.substring(beginIndex).indexOf(word);
	}

	public String GetIPAddress() {
		try {
			return InetAddress.getLocalHost().toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public String getStackTrace(final Throwable throwable) {
		final StringWriter sw = new StringWriter();
		final PrintWriter pw = new PrintWriter(sw, true);
		throwable.printStackTrace(pw);
		// System.out.println(sw.getBuffer().toString());
		return sw.getBuffer().toString();
	}

	@SuppressWarnings("unused")
	public String VersionString(int indexOfVersion, String doc, String component) {
		String version = null;
		switch (component) {
		case "zookeeper":
			version = doc.substring(indexOfVersion, indexOfVersion + 10);
			break;
		case "kafka":
			version = doc.substring(indexOfVersion, indexOfVersion + 10);
			break;
		case "hbase":
			version = doc.substring(indexOfVersion, indexOfVersion + 10);
			break;
		case "java":
			version = doc.substring(indexOfVersion, indexOfVersion + 10);
			break;
		case "scala":
			version = doc.substring(indexOfVersion, indexOfVersion + 10);
			break;
		default:
			version = "-1";
		}
		return null;
	}
}
