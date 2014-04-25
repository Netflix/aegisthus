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
package com.netflix.aegisthus.io.sstable;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.CounterColumn;
import org.apache.cassandra.db.DeletedColumn;
import org.apache.cassandra.db.ExpiringColumn;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.sstable.Descriptor;

import com.google.common.collect.Maps;

/**
 * This class serializes each row in a SSTable to a string.
 * 
 * <pre>
 * SSTableScanner scanner = new SSTableScanner(&quot;columnfamily-g-1-Data.db&quot;);
 * while (scanner.hasNext()) {
 * 	String row = scanner.next();
 * 	System.out.println(jsonRow);
 * }
 * </pre>
 */
@SuppressWarnings("rawtypes")
public class SSTableScanner extends SSTableReader implements Iterator<String> {
	public static final String COLUMN_NAME_KEY = "$$CNK$$";
	public static final String KEY = "$$KEY$$";
	public static final String SUB_KEY = "$$SUB_KEY$$";

	private AbstractType columnNameConvertor = null;

	private Map<String, AbstractType> converters = Maps.newHashMap();
	private AbstractType keyConvertor = null;
	private Descriptor.Version version = null;
	private final OnDiskAtom.Serializer serializer = new OnDiskAtom.Serializer(new ColumnSerializer());
	private long maxColSize = -1;
	private long errorRowCount = 0;

	public SSTableScanner(DataInput input, Descriptor.Version version) {
		this(input, null, -1, version);
	}

	public SSTableScanner(DataInput input, Map<String, AbstractType> converters, Descriptor.Version version) {
		this(input, converters, -1, version);
	}

	public SSTableScanner(DataInput input,
			Map<String, AbstractType> converters,
			long end,
			Descriptor.Version version) {
		init(input, converters, end);
		this.version = version;
	}

	public SSTableScanner(String filename) {
		this(filename, null, 0, -1);
	}

	public SSTableScanner(String filename, long start) {
		this(filename, null, start, -1);
	}

	public SSTableScanner(String filename, long start, long end) {
		this(filename, null, start, end);
	}

	public SSTableScanner(String filename, Map<String, AbstractType> converters, long start) {
		this(filename, converters, start, -1);
	}

	public SSTableScanner(String filename, Map<String, AbstractType> converters, long start, long end) {
		try {
			this.version = Descriptor.fromFilename(filename).version;
			init(new DataInputStream(new BufferedInputStream(new FileInputStream(filename), 65536 * 10)), converters,
					end);
			if (start != 0) {
				skipUnsafe(start);
				this.pos = start;
			}
		} catch (IOException e) {
			throw new IOError(e);
		}
	}

	public void close() {
		if (input != null) {
			try {
				((DataInputStream) input).close();
			} catch (IOException e) {
				// ignore
			}
			input = null;
		}
	}

	private AbstractType getConvertor(String convertor) {
		return getConvertor(convertor, BytesType.instance);
	}

	private AbstractType getConvertor(String convertor, AbstractType defaultType) {
		if (converters != null && converters.containsKey(convertor)) {
			return converters.get(convertor);
		}
		return defaultType;
	}

	public long getDatasize() {
		return datasize;
	}

	@Override
	public boolean hasNext() {
		return hasMore();
	}

	protected void init(DataInput input, Map<String, AbstractType> converters, long end) {
		this.input = input;
		if (converters != null) {
			this.converters = converters;
		}
		this.end = end;

		this.columnNameConvertor = getConvertor(COLUMN_NAME_KEY);
		this.keyConvertor = getConvertor(KEY);
	}

	protected void insertKey(StringBuilder bf, String value) {
		bf.append("\"").append(value).append("\": ");
	}

	/**
	 * TODO: Change this to use AegisthusSerializer
	 * 
	 * Returns json for the next row in the sstable. <br/>
	 * <br/>
	 * 
	 * <pre>
	 * ColumnFamily:
	 * {key: {columns: [[col1], ... [colN]], deletedAt: timestamp}}
	 * </pre>
	 */
	@Override
	public String next() {
		StringBuilder str = new StringBuilder();
		try {
			int keysize = input.readUnsignedShort();
			byte[] b = new byte[keysize];
			input.readFully(b);
			String key = keyConvertor.getString(ByteBuffer.wrap(b));
			datasize = input.readLong() + keysize + 2 + 8;
			this.pos += datasize;
			int bfsize = 0;
			int idxsize = 0;
			int indexLengthSize = 0;
			if (!version.hasPromotedIndexes) {
				indexLengthSize = 8;
				if (input instanceof DataInputStream) {
					// skip bloom filter
					bfsize = input.readInt();
					skip(bfsize);
					// skip index
					idxsize = input.readInt();
					skip(idxsize);
				} else {
					// skip bloom filter
					bfsize = input.readInt();
					input.skipBytes(bfsize);
					// skip index
					idxsize = input.readInt();
					input.skipBytes(idxsize);
				}
			}
			// The local deletion times are similar to the times that they were
			// marked for delete, but we only
			// care to know that it was deleted at all, so we will go with the
			// long value as the timestamps for
			// update are long as well.
			@SuppressWarnings("unused")
			int localDeletionTime = input.readInt();
			long markedForDeleteAt = input.readLong();
			int columnCount = input.readInt();
			long columnsize = datasize - keysize - 2 /* byte for keysize */
					- 8 /* long for data size */
					- bfsize  /* int for bloom filter size */
					- idxsize  /* int for index size */
					- indexLengthSize /* 0 or 8 depending on promoted indexes */
					- 4 /* local deletetion time */
					- 8 /* marked for delete */
					- 4 /* column count */;
			str.append("{");
			insertKey(str, key);
			str.append("{");
			insertKey(str, "deletedAt");
			str.append(markedForDeleteAt);
			str.append(", ");
			insertKey(str, "columns");
			str.append("[");
			if (maxColSize == -1 || columnsize < maxColSize) {
				serializeColumns(str, columnCount, input);
			} else {
				errorRowCount++;
				String msg = String.format("[\"error\",\"row too large: %,d bytes - limit %,d bytes\",0]", datasize,
						maxColSize);
				str.append(msg);
				skipUnsafe((int) columnsize);
			}
			str.append("]");
			str.append("}}\n");
		} catch (IOException e) {
			throw new IOError(e);
		}

		return str.toString();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	public void serializeColumns(StringBuilder sb, int count, DataInput columns) throws IOException {
		for (int i = 0; i < count; i++) {
			// serialize columns
			OnDiskAtom atom = serializer.deserializeFromSSTable(columns, version);
			if (atom instanceof IColumn) {
				IColumn column = (IColumn) atom;
				String cn = convertColumnName(column.name());
				sb.append("[\"");
				sb.append(cn);
				sb.append("\", \"");
				sb.append(getConvertor(cn).getString(column.value()));
				sb.append("\", ");
				sb.append(column.timestamp());

				if (column instanceof DeletedColumn) {
					sb.append(", ");
					sb.append("\"d\"");
				} else if (column instanceof ExpiringColumn) {
					sb.append(", ");
					sb.append("\"e\"");
					sb.append(", ");
					sb.append(((ExpiringColumn) column).getTimeToLive());
					sb.append(", ");
					sb.append(column.getLocalDeletionTime());
				} else if (column instanceof CounterColumn) {
					sb.append(", ");
					sb.append("\"c\"");
					sb.append(", ");
					sb.append(((CounterColumn) column).timestampOfLastDelete());
				}
				sb.append("]");
				if (i < count - 1) {
					sb.append(", ");
				}
			} else if (atom instanceof RangeTombstone) {
				// RangeTombstones need to be held so that we can handle them
				// during reduce. Currently aegisthus doesn't handle this well as we
				// aren't handling columns in the order they should be sorted. We will
				// have to change that in the near future.
				/*
				 * RangeTombstone rt = (RangeTombstone) atom; sb.append("[\"");
				 * sb.append(convertColumnName(rt.name())); sb.append("\", \"");
				 * sb.append(convertColumnName(rt.min)); sb.append("\", ");
				 * sb.append(rt.data.markedForDeleteAt); sb.append(", \"");
				 * sb.append(convertColumnName(rt.max)); sb.append("\"]");
				 */
			} else {
				throw new IOException("column unexpected type");
			}
		}
	}

	private String convertColumnName(ByteBuffer bb) {
		return columnNameConvertor.getString(bb).replaceAll("[\\s\\p{Cntrl}]", " ").replace("\\", "\\\\").replace("\"", "\\\"");
	}

	public long getErrorRowCount() {
		return this.errorRowCount;
	}

	public void setMaxColSize(long maxColSize) {
		this.maxColSize = maxColSize;
	}
}
