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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

public class SSTableReader {

	protected long datasize;
	protected DataInput input;
	protected InputStream is;
	protected boolean supercolumn;

	long end = -1;
	long pos = 0;

	public SSTableReader() {
		super();
	}

	protected boolean hasMore() {
		try {
			if (input instanceof RandomAccessFile) {
				RandomAccessFile file = (RandomAccessFile) input;
				return file.getFilePointer() != file.length();
			}
			if (end == -1) {
				return ((DataInputStream) input).available() != 0;
			} else {
				return pos < end;
			}
		} catch (IOException e) {
			throw new IOError(e);
		}
	}

	public void skip(int bytes) throws IOException {
		byte[] b = new byte[bytes];
		input.readFully(b);
	}

	/**
	 * This method is unsafe because it doesn't work on stdin in some cases.
	 * 
	 * @param bytes
	 * @throws IOException
	 */
	public void skipUnsafe(int bytes) throws IOException {
		int skipped = 0;
		while (skipped < bytes) {
			skipped += input.skipBytes(bytes - skipped);
		}
	}

	public void skipUnsafe(long bytes) throws IOException {
	    pos = bytes;
	    if (is != null) {
	        long skipped = 0;
	        while (skipped < bytes) {
	            long actual = is.skip(bytes - skipped);
	            if (actual < 0) {
	                throw new IOException("skip returned negative");
	            }
	            skipped += actual;
	        }
	        return;
	    }
		while (bytes > Integer.MAX_VALUE) {
			skipUnsafe(Integer.MAX_VALUE);
			bytes -= Integer.MAX_VALUE;
		}
		skipUnsafe((int) bytes);
	}

}