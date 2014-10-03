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

import org.apache.cassandra.db.CounterColumn;
import org.apache.cassandra.db.DeletedColumn;
import org.apache.cassandra.db.ExpiringColumn;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.sstable.Descriptor;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOG = LoggerFactory.getLogger(SSTableScanner.class);
    public static final String COLUMN_NAME_KEY = "$$CNK$$";
	public static final String KEY = "$$KEY$$";
	public static final String SUB_KEY = "$$SUB_KEY$$";

	private AbstractType columnNameConvertor = null;

	private Map<String, AbstractType> converters = Maps.newHashMap();
	private AbstractType keyConvertor = null;
	private Descriptor.Version version = null;
	private final OnDiskAtom.Serializer serializer = OnDiskAtom.Serializer.instance;
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
			long columnsize = 0L;
			int columnCount = Integer.MAX_VALUE;

			input.readFully(b);
			String key = keyConvertor.getString(ByteBuffer.wrap(b));
			// The unsigned short that contains the keysize and the keysize
			datasize = keysize + 2;

			if (version.hasRowSizeAndColumnCount) {
				columnsize = input.readLong();
				datasize += columnsize + 8; // column size + the long that held the column size
			}
			this.pos += datasize;

			// The local deletion times are similar to the times that they were
			// marked for delete, but we only
			// care to know that it was deleted at all, so we will go with the
			// long value as the timestamps for
			// update are long as well.
			@SuppressWarnings("unused")
			int localDeletionTime = input.readInt();
			long markedForDeleteAt = input.readLong();

			if (version.hasRowSizeAndColumnCount) {
				columnCount = input.readInt();
			}

			str.append("{");
			insertKey(str, key);
			str.append("{");
			insertKey(str, "deletedAt");
			str.append(markedForDeleteAt);
			str.append(", ");
			insertKey(str, "columns");
			str.append("[");
			if (maxColSize == -1 || columnsize < maxColSize) {
				long size = serializeColumns(str, columnCount, input);
				if (!version.hasRowSizeAndColumnCount) {
					datasize += size;
				}
			} else {
				errorRowCount++;
				str.append(rowTooLargeErrorMsg(datasize, maxColSize));
				skipUnsafe((int) columnsize);
			}
			str.append("]");
			str.append("}}\n");
		} catch (IOException e) {
            LOG.error("Failing due to error", e);
			throw new IOError(e);
		}

		return str.toString();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	/** returns colsize if it were a Cassandra 2.0 sstable */
	public long serializeColumns(StringBuilder sb, int count, DataInput columns) throws IOException {
		int sbStartPos = sb.length();	// in case we need to rewind later (large column)
		boolean moreThanOne = false;
		boolean skip = false;
		long colsize = 2L;		// END_OF_ROW marker is 2 bytes

		// this method is tricky, since in 2.0 we don't actually have a column count.
		for (int i = 0; i < count; i++) {
			// serialize columns
			OnDiskAtom atom = serializer.deserializeFromSSTable(columns, version);
			if (atom == null) {
				break;
			}

			colsize += atom.serializedSizeForSSTable();

			if (skip) {
				continue;
			} else if (maxColSize != -1 && colsize >= maxColSize) {
				skip = true;
				sb.delete(sbStartPos, sb.length());
				continue;
			}

			if (atom instanceof Column) {
				Column column = (Column) atom;
				String cn = convertColumnName(column.name());

				if (moreThanOne) {
					sb.append(", ");
				} else {
					moreThanOne = true;
				}
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
				LOG.debug("Skipping RangedTombstone in JSON output");
			} else {
				throw new IOException("column unexpected type");
			}
		}

		if (skip) {
			errorRowCount++;
			sb.append(rowTooLargeErrorMsg(colsize, maxColSize));
		}

		return colsize;
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

	private String rowTooLargeErrorMsg(long datasize, long maxColSize) {
		return String.format("[\"error\",\"row too large: %,d bytes - limit %,d bytes\",0]", datasize,
						maxColSize);
	}
}
