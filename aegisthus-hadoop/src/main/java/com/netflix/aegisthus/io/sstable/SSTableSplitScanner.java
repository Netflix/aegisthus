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
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOError;
import java.io.IOException;
import java.util.Iterator;

/**
 * This class scans a SSTable and returns the size of each row.
 */
public class SSTableSplitScanner extends SSTableReader implements Iterator<Integer> {

	public SSTableSplitScanner(String filename) {
		try {
			this.input = new DataInputStream(new BufferedInputStream(new FileInputStream(filename), 65536 * 10));
		} catch (IOException e) {
			throw new IOError(e);
		}
	}

	@Override
	public boolean hasNext() {
		return hasMore();
	}

	@Override
	public Integer next() {
		int keysize = 0;
		try {
			keysize = input.readUnsignedShort();
			skipUnsafe(keysize);
			datasize = input.readLong();
			skipUnsafe((int) datasize);
		} catch (IOException e) {
			throw new IOError(e);
		}
		datasize = datasize + keysize + 2 + 8;
		pos += datasize;
		//keysize + datasize + 2 for the length of keysize + 8 for length of data
		return (int) datasize;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
