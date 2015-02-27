package org.kiteq.commons.util;

import java.util.HashMap;
import java.util.Map;

/**
 * @author gaofeihang
 * @since Feb 25, 2015
 */
public class ParamUtils {
	
	/**
	 * -max 10 -test
	 * <br>converts to<br>
	 * {-max=10, -test=true}
	 */
    public static Map<String, String> parse(String[] args) {
        Map<String, String> params = new HashMap<String, String>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("-")) {
                if (i + 1 < args.length && !args[i + 1].startsWith("-")) {
                    params.put(args[i], args[i + 1]);
                } else {
                    params.put(args[i], "true");
                }
            }
        }
        return params;
    }
	
	/**
	 * helloAction?name=guest&age=3
	 * <br>converts to<br>
	 * {name=guest, age=3}
	 */
    public static Map<String, String> parse(String uri) {
        String paramString = uri.substring(uri.indexOf("?") + 1);
        String[] keyValues = paramString.split("&");
        Map<String, String> params = new HashMap<String, String>();
        if (keyValues.length > 0) {
            for (String keyValue : keyValues) {
                String[] parts = keyValue.split("=");
                if (parts.length == 2) {
                    params.put(parts[0], parts[1]);
                }
            }
        }
        return params;
    }

}
