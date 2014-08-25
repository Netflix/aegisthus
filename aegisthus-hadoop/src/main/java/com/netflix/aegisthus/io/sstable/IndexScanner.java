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
import java.util.Iterator;

import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

public class IndexScanner implements Iterator<Pair<String, Long>> {
	private DataInput input;
	private Descriptor.Version version = null;

	public IndexScanner(String filename) {
		try {
			this.input = new DataInputStream(new BufferedInputStream(new FileInputStream(filename), 65536 * 10));
			this.version = Descriptor.fromFilename(filename).version;
		} catch (IOException e) {
			throw new IOError(e);
		}
	}

	public IndexScanner(DataInput input, Descriptor.Version version) {
		this.input = input;
		this.version = version;
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

	@Override
	public boolean hasNext() {
		try {
			return ((DataInputStream) input).available() != 0;
		} catch (IOException e) {
			throw new IOError(e);
		}
	}

	@Override
	public Pair<String, Long> next() {
		try {
			String key = BytesType.instance.getString(ByteBufferUtil.readWithShortLength(input));
			Long offset = input.readLong();
			if (version.hasPromotedIndexes) {
				skipPromotedIndexes();
			}
			return Pair.create(key, offset);
		} catch (IOException e) {
			throw new IOError(e);
		}
	}

	protected void skipPromotedIndexes() throws IOException {
		int size = input.readInt();
		if (size <= 0) {
			return;
		}

		FileUtils.skipBytesFully(input, size);
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
