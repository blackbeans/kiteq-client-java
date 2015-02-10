package org.kiteq.commons.util;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * @author gaofeihang
 * @since Jan 31, 2015
 */
public class JsonUtils {

	private static final Logger logger = LoggerFactory.getLogger(JsonUtils.class);

	private static ThreadLocal<ObjectMapper> objMapperLocal = new ThreadLocal<ObjectMapper>() {
		@Override
		public ObjectMapper initialValue() {
			return new ObjectMapper();
		}
	};
	
	private static ThreadLocal<ObjectMapper> prettyObjMapperLocal = new ThreadLocal<ObjectMapper>() {
		@Override
		public ObjectMapper initialValue() {
			return new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
		}
	};

	public static String toJSON(Object value) {
		String result = null;
		try {
			result = objMapperLocal.get().writeValueAsString(value);
		} catch (Exception e) {
			logger.error("toJSON error: {}", value, e);
		}
		// fix null string
		if ("null".equals(result)) {
			result = null;
		}
		return result;
	}

	public static <T> T toT(String jsonString, Class<T> clazz) {
		try {
			return objMapperLocal.get().readValue(jsonString, clazz);
		} catch (Exception e) {
		    logger.error("toT error: {}", jsonString, e);
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public static Map<String, Object> toMap(String jsonString) {
		return toT(jsonString, Map.class);
	}

	public static String prettyPrint(Object value) {
		String result = null;
		try {
			result = prettyObjMapperLocal.get().writeValueAsString(value);
		} catch (Exception e) {
			logger.error("prettyPrint error: {}", value, e);
		}
		// fix null string
		if ("null".equals(result)) {
			result = null;
		}
		return result;
	}

}
