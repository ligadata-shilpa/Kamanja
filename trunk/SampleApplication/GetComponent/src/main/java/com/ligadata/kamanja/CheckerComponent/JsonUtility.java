package com.ligadata.kamanja.CheckerComponent;

import org.json.simple.JSONArray;
//import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class JsonUtility {

	String component;
	String hostList;
	String authentication;
	String masterPrincipal;
	String regionServer;
	String keyType;
	String principal;
	JSONObject jsonObject;
	JSONParser jsonParser;

	public void JsonParse(String json) throws ParseException {
		// String json =
		// "{\"component\":\"zookeeper\",\"hostlist\":\"192.168.10.20:2181,192.168.10.21:2181\"}";
		// String json =
		// "{\"component\":\"zookeeper\",\"hostlist\":\"localhost:2181\"}";
		// System.out.println(json);
		jsonParser = new JSONParser();
		jsonObject = (JSONObject) jsonParser.parse(json);
		component = (String) jsonObject.get("component");
		if (component == null)
			component = (String) jsonObject.get("StoreType");
		hostList = (String) jsonObject.get("hostlist");
		if (hostList == null)
			hostList = (String) jsonObject.get("Location");
	}

	public JSONArray GetJsonArray(String json) throws ParseException {
		JSONParser jsonParser = new JSONParser();
		JSONArray jsonArray = (JSONArray) jsonParser.parse(json);
		return jsonArray;
	}

	public void ParseOptionalField() {
		//if (component.equalsIgnoreCase("hbase")) {
			authentication = (String) jsonObject.get("authentication");
			masterPrincipal = (String) jsonObject.get("master_principal");
			regionServer = (String) jsonObject.get("regionserver_principal");
			principal = (String) jsonObject.get("principal");
			keyType = (String) jsonObject.get("keytab");
		//}
	}

	public String getPrincipal() {
		return principal;
	}

	public void setPrincipal(String principal) {
		this.principal = principal;
	}

	public String GetComponent() {
		return component;
	}

	public String getComponent() {
		return component;
	}

	public void setComponent(String component) {
		this.component = component;
	}

	public String getHostList() {
		return hostList;
	}

	public void setHostList(String hostList) {
		this.hostList = hostList;
	}

	public String getAuthentication() {
		return authentication;
	}

	public void setAuthentication(String hbaseAuthentication) {
		this.authentication = hbaseAuthentication;
	}

	public String getMasterPrincipal() {
		return masterPrincipal;
	}

	public void setMasterPrincipal(String masterPrincipal) {
		this.masterPrincipal = masterPrincipal;
	}

	public String getRegionServer() {
		return regionServer;
	}

	public void setRegionServer(String regionServer) {
		this.regionServer = regionServer;
	}

	public String getKeyType() {
		return keyType;
	}

	public void setKeyType(String keyType) {
		this.keyType = keyType;
	}

	public String GetHostList() {
		return hostList;
	}

	@SuppressWarnings("unchecked")
	public JSONObject Serialize(String comp, String host, String version, String status, String errorMessage) {
		JSONObject jsonObj = new JSONObject();
		jsonObj.put("component", comp);
		jsonObj.put("nodeId", host);
		jsonObj.put("version", version);
		jsonObj.put("status", status);
		jsonObj.put("error message", errorMessage);
		// System.out.println(jsonObj);
		return jsonObj;
	}
}
