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
package com.netflix.aegisthus.io.commitlog;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOError;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.utils.ByteBufferUtil;

import com.google.common.collect.Lists;
import com.netflix.aegisthus.io.sstable.SSTableScanner;

/**
 * This code is experimental.
 */
public class CommitLogScanner extends SSTableScanner {
	protected List<String> cache;
	protected StringBuilder sb;

	@SuppressWarnings("rawtypes")
	public CommitLogScanner(DataInput di, Map<String, AbstractType> convertors) {
		super(di, convertors, -1, true);
		cache = Lists.newLinkedList();
	}

	@SuppressWarnings("unused")
	public String next(int filter) {
		int serializedSize;
		if (cache.size() > 0) {
			// if we are here we are reading rows that we already reported the
			// size of.
			datasize = 0;
			return cache.remove(0);
		}
		try {
			outer: while (true) {
				serializedSize = input.readInt();
				if (serializedSize == 0) {
					return null;
				}
				long claimedSizeChecksum = input.readLong();
				byte[] buffer = new byte[(int) (serializedSize * 1.2)];
				input.readFully(buffer, 0, serializedSize);
				long claimedChecksum = input.readLong();

				// two checksums plus the int for the size
				datasize = serializedSize + 8 + 8 + 4;

				FastByteArrayInputStream bufIn = new FastByteArrayInputStream(buffer, 0, serializedSize);
				DataInput ris = new DataInputStream(bufIn);
				String table = ris.readUTF();
				ByteBuffer key = ByteBufferUtil.readWithShortLength(ris);
				Map<Integer, ColumnFamily> modifications = new HashMap<Integer, ColumnFamily>();
				int size = ris.readInt();
				for (int i = 0; i < size; ++i) {
					Integer cfid = Integer.valueOf(ris.readInt());
					if (filter >= 0 && cfid != filter) {
						continue outer;
					}
					if (!ris.readBoolean()) {
						continue;
					}
					cfid = Integer.valueOf(ris.readInt());
					int localDeletionTime = ris.readInt();
					long markedForDeleteAt = ris.readLong();
					sb = new StringBuilder();
					sb.append("{");
					insertKey(sb, BytesType.instance.getString(key));
					sb.append("{");
					insertKey(sb, "deletedAt");
					sb.append(markedForDeleteAt);
					sb.append(", ");
					insertKey(sb, "columns");
					sb.append("[");
					int columns = ris.readInt();
					serializeColumns(sb, columns, ris);
					sb.append("]");
					sb.append("}}");
					cache.add(sb.toString());
				}
				if (cache.size() > 0) {
					return cache.remove(0);
				} else {
					return null;
				}
			}
		} catch (Exception e) {
			throw new IOError(e);
		}
	}
}
