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
import java.io.InputStream;
import java.util.Iterator;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.io.input.CountingInputStream;

/**
 * This class reads an SSTable index file and returns the offset for each key.
 * If you want to know the key related with each offset use {@link IndexScanner}
 */
public class OffsetScanner implements Iterator<Pair<Long, Long>> {
	private DataInput input;
	private Descriptor.Version version = null;
	private CountingInputStream countingInputStream;

	public OffsetScanner(DataInput input, Descriptor.Version version) {
		this.input = input;
		this.version = version;
	}

	public OffsetScanner(InputStream is, String filename) {
		this.countingInputStream = new CountingInputStream(is);
		this.input = new DataInputStream(this.countingInputStream);
		this.version = Descriptor.fromFilename(filename).version;
	}

	public OffsetScanner(String filename) {
		try {
			this.input = new DataInputStream(new BufferedInputStream(new FileInputStream(filename), 65536 * 10));
			this.version = Descriptor.fromFilename(filename).version;
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

	@Override
	public boolean hasNext() {
		try {
			return ((DataInputStream) input).available() != 0;
		} catch (IOException e) {
			throw new IOError(e);
		}
	}

	/**
	 * Returns a Pair<Long, Long> where the pair is <offset in data file, offset in the index file>
	 **/
	@Override
	public Pair<Long, Long> next() {
		try {
			long indexOffset = countingInputStream.getCount();
			int keysize = input.readUnsignedShort();
			input.skipBytes(keysize);
			Long offset = input.readLong();
			if (version.hasPromotedIndexes) {
				skipPromotedIndexes();
			}
			return Pair.create(offset, indexOffset);
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