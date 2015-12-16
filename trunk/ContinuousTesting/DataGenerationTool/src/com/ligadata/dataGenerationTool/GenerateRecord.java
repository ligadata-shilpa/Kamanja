package com.ligadata.dataGenerationTool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.ligadata.dataGenerationTool.fields.CopsFields;
import com.ligadata.dataGenerationTool.fields.CopsFields.EventSource;

public class GenerateRecord {

	public String GenerateHit(HashMap<String, String> record, String Delimiter) {
		String hit = "";
		CopsFields copsFields = new CopsFields();
		String[] enumKeys = { "eventsource", "action", "resourceprotocol",
				"authenticationmethod", "resourcetype",
				"confidentialdatalabels", "userrole" };
		boolean LineAlreadyWritten;
		// Get a set of the entries
		Set<Entry<String, String>> set = record.entrySet();
		// Get an iterator
		Iterator<Entry<String, String>> i = set.iterator();
		// Display elements
		while (i.hasNext()) {
			LineAlreadyWritten=false;
			Map.Entry<String, String> line = i.next();
			for (String str : enumKeys) {
				if (line.getKey().trim().equalsIgnoreCase(str)) {
					String val = copsFields.EnumLookup(str).toString();
					hit = hit + line.getKey() + Delimiter + val + Delimiter;
					LineAlreadyWritten = true;
					 break;
				}
			}
			if (LineAlreadyWritten == false) {
				hit = hit + line.getKey() + Delimiter + line.getValue()
						+ Delimiter;
				LineAlreadyWritten = false;
			}
		}
		if (hit.endsWith(Delimiter)) {
			hit = hit.substring(0, hit.length() - 1);
		}
		return hit;

	}
}
