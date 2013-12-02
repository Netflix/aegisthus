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
package com.netflix.aegisthus.mapred.reduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.aegisthus.tools.AegisthusSerializer;

public class CassReducer extends Reducer<Text, Text, Text, Text> {
	public static final AegisthusSerializer as = new AegisthusSerializer();
	Set<Text> valuesSet = new HashSet<Text>();

	@SuppressWarnings("unchecked")
	protected Long getTimestamp(Map<String, Object> map, String columnName) {
		return (Long) ((List<Object>) map.get(columnName)).get(2);
	}
	
	public void reduce(Text key, Iterable<Text> values, Context ctx)
			throws IOException, InterruptedException {
		Map<String, Object> columns = null;
		Object rowKey = null;
		Long deletedAt = Long.MIN_VALUE;
		// If we only have one distinct value we don't need to process
		// differences, which is slow and should be avoided.
		valuesSet.clear();
		for (Text value :values) {
			valuesSet.add(new Text(value));
		}
		if (valuesSet.size() == 1) {
			ctx.write(key, new Text(valuesSet.iterator().next().toString()));
			return;
		}
		for (Text val : valuesSet) {
			Map<String, Object> map = as.deserialize(val.toString());
			// The json has one key value pair, the data is always under the
			// first key so do it once to save lookup
			rowKey = map.remove(AegisthusSerializer.KEY);
			Long curDeletedAt = (Long) map.remove(AegisthusSerializer.DELETEDAT);
			if (curDeletedAt > deletedAt) {
				deletedAt = curDeletedAt;
			}
			if (columns == null) {
				columns = Maps.newTreeMap();
				columns.putAll(map);
			} else {
				Set<String> columnNames = Sets.newHashSet();
				columnNames.addAll(map.keySet());
				columnNames.addAll(columns.keySet());

				for (String columnName : columnNames) {
					boolean oldKey = columns.containsKey(columnName);
					boolean newKey = map.containsKey(columnName);
					if (oldKey && newKey) {
						if (getTimestamp(map, columnName) > getTimestamp(columns, columnName)) {
							columns.put(columnName, map.get(columnName));
						}
					} else if (newKey) {
						columns.put(columnName, map.get(columnName));
					}
				}
			}
		}
		// When cassandra compacts it removes columns that are in deleted rows
		// that are older than the deleted timestamp.
		// we will duplicate this behavior. If the etl needs this data at some
		// point we can change, but it is only available assuming
		// cassandra hasn't discarded it.
		List<String> delete = Lists.newArrayList();
		for (Map.Entry<String, Object> e : columns.entrySet()) {
			@SuppressWarnings("unchecked")
			Long ts = (Long) ((List<Object>) e.getValue()).get(2);
			if (ts < deletedAt) {
				delete.add(e.getKey());
			}
		}

		for (String k : delete) {
			columns.remove(k);
		}

		columns.put(AegisthusSerializer.DELETEDAT, deletedAt);
		columns.put(AegisthusSerializer.KEY, rowKey);
		ctx.write(key, new Text(AegisthusSerializer.serialize(columns)));
	
	}


}
