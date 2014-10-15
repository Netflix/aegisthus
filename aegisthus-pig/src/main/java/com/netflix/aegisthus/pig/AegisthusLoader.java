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
package com.netflix.aegisthus.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.codehaus.jackson.JsonParseException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.aegisthus.tools.AegisthusSerializer;

/**
 * Pig loader for aegisthus json format.
 */
public class AegisthusLoader extends PigStorage implements LoadMetadata {
	private static final String REQUIRED_COLUMNS = "required.columns";
	protected static final BagFactory bagFactory = BagFactory.getInstance();
	protected static final TupleFactory tupleFactory = TupleFactory.getInstance();
	protected boolean clean = true;
	private boolean mRequiredColumnsInitialized = false;

	@SuppressWarnings("rawtypes")
	protected RecordReader reader = null;

	private AegisthusSerializer serializer;

	public AegisthusLoader() {
	}

	public AegisthusLoader(String clean) {
		this.clean = Boolean.valueOf(clean);
	}

	/**
	 * This function removes all the deleted columns from the data. If the
	 * columns are deleted via the row being deleted, that will be processed
	 * prior to the data being available in json.
	 */
	@SuppressWarnings("unchecked")
	private void cleanse(Map<String, Object> map) {
		long deletedAt = (Long) map.get(AegisthusSerializer.DELETEDAT);
		List<String> delete = Lists.newArrayList();
		for (Map.Entry<String, Object> e : map.entrySet()) {
			if (!(AegisthusSerializer.KEY.equals(e.getKey()) || AegisthusSerializer.DELETEDAT.equals(e.getKey()))) {
				List<Object> values = (List<Object>) e.getValue();
				// d in the 4th position indicates the column has been deleted
				if (deletedAt > (Long) values.get(2) || (values.size() > 3 && "d".equals(values.get(3)))) {
					delete.add(e.getKey());
				}
			}
		}
		for (String key : delete) {
			map.remove(key);
		}
	}

	@Override
	public LoadCaster getLoadCaster() throws IOException {
		return new AegisthusLoadCaster();
	}

	@Override
	public Tuple getNext() throws IOException {
		if (!mRequiredColumnsInitialized) {
			if (signature != null) {
				mRequiredColumns = (boolean[]) ObjectSerializer.deserialize(getUdfProperty(REQUIRED_COLUMNS));
			}
			mRequiredColumnsInitialized = true;
		}
		if (reader == null) {
			return null;
		}
		if (serializer == null) {
			serializer = new AegisthusSerializer();
		}
		try {
			while (reader.nextKeyValue()) {
				Text value = (Text) reader.getCurrentValue();
				String s = value.toString();
				if (s.contains("\t")) {
					s = s.split("\t")[1];
				}
				Map<String, Object> map = serializer.deserialize(s);
				if (clean) {
					cleanse(map);
					// when clean if we have an empty row we will ignore it. The
					// map will be size 2 because it will only
					// have the key and the deleted ts

					// TODO: only remove row if it is empty and is deleted.
					if (map.size() == 2) {
						continue;
					}
				}
				return tuple(map);
			}
		} catch (InterruptedException e) {
			// ignore
		}

		return null;
	}

	@Override
	public String[] getPartitionKeys(String arg0, Job arg1) throws IOException {
		return null;
	}

	protected ResourceFieldSchema field(String name, byte type) {
		ResourceFieldSchema fs = new ResourceFieldSchema();
		fs.setName(name);
		fs.setType(type);
		return fs;
	}

	protected ResourceFieldSchema subfield(String name, byte type, ResourceSchema schema) throws IOException {
		ResourceFieldSchema fs = new ResourceFieldSchema();
		fs.setName(name);
		fs.setType(type);
		fs.setSchema(schema);
		return fs;
	}

	protected ResourceSchema columnSchema() throws IOException {
		ResourceSchema schema = new ResourceSchema();
		List<ResourceFieldSchema> fields = new ArrayList<>();

		fields.add(field("name", DataType.BYTEARRAY));
		fields.add(field("value", DataType.BYTEARRAY));
		fields.add(field("ts", DataType.LONG));
		fields.add(field("status", DataType.CHARARRAY));
		fields.add(field("ttl", DataType.LONG));

		ResourceSchema tuple = new ResourceSchema();
		tuple.setFields(fields.toArray(new ResourceFieldSchema[0]));

		ResourceFieldSchema fs = new ResourceFieldSchema();
		fs.setName("column");
		fs.setType(DataType.TUPLE);

		fs.setSchema(tuple);
		fields.clear();
		fields.add(fs);
		schema.setFields(fields.toArray(new ResourceFieldSchema[0]));

		return schema;
	}

	@Override
	public ResourceSchema getSchema(String location, Job job) throws IOException {
		ResourceSchema resourceSchema = new ResourceSchema();
		List<ResourceFieldSchema> fields = new ArrayList<>();
		fields.add(field("key", DataType.BYTEARRAY));
		fields.add(field("deletedat", DataType.LONG));
		fields.add(subfield("map_columns", DataType.MAP, columnSchema()));
		fields.add(subfield("bag_columns", DataType.BAG, columnSchema()));
		resourceSchema.setFields(fields.toArray(new ResourceFieldSchema[0]));
		return resourceSchema;
	}

	@Override
	public ResourceStatistics getStatistics(String arg0, Job arg1) throws IOException {
		return null;
	}

	protected Properties getUdfContext(String name) {
		return UDFContext.getUDFContext().getUDFProperties(this.getClass(), new String[] { name });
	}

	protected String getUdfProperty(String property) {
		return getUdfContext(signature).getProperty(property);
	}

	@Override
	public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader reader, PigSplit split) {
		this.reader = reader;
	}

	@Override
	public RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldList) throws FrontendException {
		if (requiredFieldList == null)
			return null;
		if (requiredFieldList.getFields() != null) {
			int lastColumn = -1;
			for (RequiredField rf : requiredFieldList.getFields()) {
				if (rf.getIndex() > lastColumn) {
					lastColumn = rf.getIndex();
				}
			}
			mRequiredColumns = new boolean[lastColumn + 1];
			for (RequiredField rf : requiredFieldList.getFields()) {
				if (rf.getIndex() != -1)
					mRequiredColumns[rf.getIndex()] = true;
			}
			try {
				setUdfProperty(REQUIRED_COLUMNS, ObjectSerializer.serialize(mRequiredColumns));
			} catch (Exception e) {
				throw new RuntimeException("Cannot serialize mRequiredColumns");
			}
		}
		return new RequiredFieldResponse(true);
	}

	protected boolean required(int pos) {
		return (mRequiredColumns == null || (mRequiredColumns.length > pos && mRequiredColumns[pos]));
	}

	protected void setUdfProperty(String property, String value) {
		getUdfContext(signature).setProperty(property, value);
	}

	@SuppressWarnings("unchecked")
	protected Tuple tuple(Map<String, Object> map) throws JsonParseException, IOException {
		List<Object> values = new ArrayList<>();
		if (required(0)) {
			values.add(map.get(AegisthusSerializer.KEY));
		}
		map.remove(AegisthusSerializer.KEY);

		if (required(1)) {
			values.add(map.get(AegisthusSerializer.DELETEDAT));
		}
		map.remove(AegisthusSerializer.DELETEDAT);

		// Each item in the map must be a tuple
		if (required(2)) {
			Map<String, Object> map2 = Maps.newHashMap();
			for (Map.Entry<String, Object> e : map.entrySet()) {
				map2.put(e.getKey(), tupleFactory.newTuple((List<Object>) e.getValue()));
			}
			values.add(map2);
		}
		if (required(3)) {
			List<Tuple> cols = Lists.newArrayList();
			for (Object obj : map.values()) {
				cols.add(tupleFactory.newTuple((List<Object>) obj));
			}
			values.add(bagFactory.newDefaultBag(cols));
		}
		return tupleFactory.newTuple(values);
	}
}
