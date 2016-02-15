package check_prerequisites;

//import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class JsonUtility {

	String component;
	String hostList;

	public void JsonParse(String json) throws ParseException {
		// String json =
		// "{\"component\":\"zookeeper\",\"hostlist\":\"192.168.10.20:2181,192.168.10.21:2181\"}";
		// String json =
		// "{\"component\":\"zookeeper\",\"hostlist\":\"localhost:2181\"}";
		System.out.println(json);
		JSONParser jsonParser = new JSONParser();
		JSONObject jsonObject = (JSONObject) jsonParser.parse(json);
		component = (String) jsonObject.get("component");
		hostList = (String) jsonObject.get("hostlist");
	}

	public String GetComponent() {
		return component;
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
		//System.out.println(jsonObj);
		return jsonObj;
	}
}
