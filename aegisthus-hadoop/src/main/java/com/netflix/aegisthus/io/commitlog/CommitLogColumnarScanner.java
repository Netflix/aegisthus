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
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.utils.ByteBufferUtil;

import rx.Subscriber;

import com.netflix.aegisthus.io.sstable.SSTableColumnScanner;
import com.netflix.aegisthus.io.writable.AtomWritable;

/**
 * This code is experimental.
 */
@SuppressWarnings("unused")
public class CommitLogColumnarScanner extends SSTableColumnScanner {
    int columnFamilyId;
	public CommitLogColumnarScanner(InputStream is, Descriptor.Version version, int columnFamilyId) {
		super(is, -1, version);
		this.columnFamilyId = columnFamilyId;
	}

    protected void deserialize(Subscriber<? super AtomWritable> subscriber) {
		int serializedSize;
		try {
			outer: while (true) {
				serializedSize = input.readInt();
				if (serializedSize == 0) {
					return;
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
					if (columnFamilyId >= 0 && cfid != columnFamilyId) {
						continue outer;
					}
					if (!ris.readBoolean()) {
						continue;
					}
					cfid = Integer.valueOf(ris.readInt());
					int localDeletionTime = ris.readInt();
					long markedForDeleteAt = ris.readLong();
					int columns = ris.readInt();
					serializeColumns(subscriber, key.array(), markedForDeleteAt, columns, ris);
				}
			}
		} catch (IOException e) {
		    subscriber.onError(e);
		}
	}
}
