/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.aegisthus.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.data.DataByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * Read and write Aegisthus Json format.
 */
public class AegisthusSerializer {
	private static final Logger LOG = LoggerFactory.getLogger(AegisthusSerializer.class);
	public static String DELETEDAT = "DELETEDAT";
	public static String KEY = "$$KEY$$";
	protected static JsonFactory jsonFactory = new JsonFactory();

	static {
		jsonFactory.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
	}

	public Object type(String value) {
		return value;
	}

	public Map<String, Object> deserialize(String data) throws IOException {
		try {
			Map<String, Object> map = new LinkedHashMap<String, Object>();
			JsonParser jp = jsonFactory.createJsonParser(data);
			jp.nextToken(); // Object
			jp.nextToken(); // key
			map.put(KEY, new DataByteArray(jp.getCurrentName().getBytes()));
			jp.nextToken(); // object
			while (jp.nextToken() != JsonToken.END_OBJECT) {
				String field = jp.getCurrentName();
				if (DELETEDAT.equals(field.toUpperCase())) {
					jp.nextToken();
					map.put(DELETEDAT, jp.getLongValue());
				} else {
					jp.nextToken();
					while (jp.nextToken() != JsonToken.END_ARRAY) {
						List<Object> cols = new ArrayList<Object>();
						jp.nextToken();
						String name = jp.getText();
						cols.add(name);
						jp.nextToken();
						cols.add(new DataByteArray(jp.getText().getBytes()));
						jp.nextToken();
						cols.add(jp.getLongValue());
						if (jp.nextToken() != JsonToken.END_ARRAY) {
							String status = jp.getText();
							cols.add(status);
							if ("e".equals(status)) {
								jp.nextToken();
								cols.add(jp.getLongValue());
								jp.nextToken();
								cols.add(jp.getLongValue());
							} else if ("c".equals(status)) {
								jp.nextToken();
								cols.add(jp.getLongValue());
							}
							jp.nextToken();
						}
						map.put(name, cols);
					}
				}
			}

			return map;
		} catch (IOException e) {
			LOG.error(data);
			throw e;
		}
	}

	protected static void insertKey(StringBuilder sb, Object value) {
		sb.append("\"");
		sb.append(value);
		sb.append("\": ");
	}

	protected static void serializeColumns(StringBuilder sb, Map<String, Object> columns) {
		int count = 0;
		for (Map.Entry<String, Object> e : columns.entrySet()) {
			if (count++ > 0) {
				sb.append(", ");
			}
			@SuppressWarnings("unchecked")
			List<Object> list = (List<Object>) e.getValue();

			sb.append("[");
			sb.append("\"").append(((String) list.get(0)).replace("\\", "\\\\").replace("\"", "\\\"")).append("\"").append(",");
			sb.append("\"").append(list.get(1)).append("\"").append(",");
			sb.append(list.get(2));
			if (list.size() > 3) {
				sb.append(",").append("\"").append(list.get(3)).append("\"");
			}
			for (int i = 4; i < list.size(); i++) {
				sb.append(",").append(list.get(i));
			}
			sb.append("]");
		}
	}

	public static String serialize(Map<String, Object> data) {
		StringBuilder str = new StringBuilder();
		str.append("{");
		insertKey(str, data.remove(KEY));
		str.append("{");
		insertKey(str, "deletedAt");
		str.append(data.remove(DELETEDAT));
		str.append(", ");
		insertKey(str, "columns");
		str.append("[");
		serializeColumns(str, data);
		str.append("]");
		str.append("}}");

		return str.toString();
	}
	
}
