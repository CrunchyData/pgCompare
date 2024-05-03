package com.crunchydata.util;

import org.json.JSONArray;
import org.json.JSONObject;

public class JsonUtility {

    public static JSONObject findOne (JSONArray ja, String key, String value) {
        /////////////////////////////////////////////////
        // Variables
        /////////////////////////////////////////////////
        JSONObject result = new JSONObject();
        result.put("count",0);

        for (int i = 0; i < ja.length(); i++) {
            if ( ja.getJSONObject(i).getString(key).toLowerCase().equals(value) ) {
                result.put("data",ja.getJSONObject(i));
                result.put("count", 1);
                result.put("location", i);
                break;
            }
        }

        return result;

    }

}
