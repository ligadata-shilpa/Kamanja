package com.ligadata.dataGenerationTool;

import java.util.*;
import java.util.Map.Entry;

public class GenerateRecord {

    public String GenerateHitAsKV(LinkedHashMap<String, String> record, String Delimiter) {
        String hit = "";
        // Get a set of the entries
        Set<Entry<String, String>> set = record.entrySet();
        // Get an iterator
        Iterator<Entry<String, String>> i = set.iterator();
        // Display elements
        while (i.hasNext()) {
            Map.Entry<String, String> line = i.next();
            hit = hit + line.getKey() + Delimiter + line.getValue() + Delimiter;
        }
        if (hit.endsWith(Delimiter)) {
            hit = hit.substring(0, hit.length() - 1);
        }
        return hit;

    }

    public String GenerateHitAsCSV(List<String> record, String Delimiter) {
        String hit = "";

        for (int i = 0; i <= record.size() - 1; i++) {
            hit = hit + record.get(i) + Delimiter;
        }
        if (hit.endsWith(Delimiter)) {
            hit = hit.substring(0, hit.length() - 1);
        }
        return hit;

    }


}
