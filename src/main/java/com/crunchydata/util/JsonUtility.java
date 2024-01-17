package com.crunchydata.util;

import org.json.JSONArray;
import org.json.JSONObject;

public class JsonUtility {

    public static JSONObject findOne (JSONArray ja, String key, String value) {
        JSONObject result = new JSONObject();
        result.put("status","fail");

        for (int i = 0; i < ja.length(); i++) {
            if ( ja.getJSONObject(i).getString(key).toLowerCase().equals(value) ) {
                result.put("data",ja.getJSONObject(i));
                result.put("status","success");
                break;
            }
        }

        return result;

    }

}
