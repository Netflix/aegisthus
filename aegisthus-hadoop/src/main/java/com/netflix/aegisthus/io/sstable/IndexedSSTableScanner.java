/**
 * Copyright 2014 Netflix, Inc.
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

import java.io.DataInput;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;

import org.apache.cassandra.io.sstable.Descriptor.Version;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.io.input.CountingInputStream;

public class IndexedSSTableScanner extends SSTableColumnScanner {
	private IndexScanner indexScanner = null;
	private Pair<String, Long> curRow = null;
	private Pair<String, Long> nextRow = null;
	private CountingInputStream is;
	private boolean checked = false;

	public IndexedSSTableScanner(InputStream is,
			long end,
			Version version,
			DataInput indexInput) {
		super(is, end, version);
		this.is = new CountingInputStream(is);
		this.indexScanner = new IndexScanner(indexInput, version);
		this.nextRow = this.indexScanner.next();
	}

	protected void checkPosition() {
	    if (!checked) {
            curRow = nextRow;
            nextRow = this.indexScanner.next();
            if (is.getByteCount() != curRow.right) {
                long bytesToSkip = curRow.right - is.getByteCount();
                while (bytesToSkip > 0) {
                    long skipped;
                    try {
                        skipped = is.skip(bytesToSkip);
                    } catch (IOException e) {
                        throw new IOError(e);
                    }
                    bytesToSkip -= skipped;
                }
            }
	    }
        checked = true;
	}

	protected boolean validateRow(String key, long datasize) {
        if (nextRow != null) {
            long expectedDatasize = nextRow.right - curRow.right;
            return (key.equals(curRow.left) &&  datasize == expectedDatasize);
        }
		return true;
	}

}
