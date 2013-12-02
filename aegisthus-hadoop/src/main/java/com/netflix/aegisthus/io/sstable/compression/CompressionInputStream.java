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
package com.netflix.aegisthus.io.sstable.compression;

import static java.lang.Math.min;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.xerial.snappy.SnappyOutputStream;

/**
 * This class implements an input stream for reading Snappy compressed data of
 * the format produced by {@link SnappyOutputStream}.
 */
public class CompressionInputStream extends InputStream {
	private final byte[] buffer;
	private boolean closed;
	private CompressionMetadata cm;

	private boolean eof;
	private final InputStream in;
	private final byte[] input;
	private int position;
	private int valid;

	/**
	 * Creates a Snappy input stream to read data from the specified underlying
	 * input stream.
	 * 
	 * @param in
	 *            the underlying input stream
	 */
	public CompressionInputStream(InputStream in, CompressionMetadata cm) throws IOException {
		this.cm = cm;
		this.in = in;
		//chunkLength*2 because there are some cases where the data is larger than specified
		input = new byte[cm.chunkLength() * 2];
		buffer = new byte[cm.chunkLength() * 2];
	}

	@Override
	public int available() throws IOException {
		if (closed) {
			return 0;
		}
		if (valid > position) {
			return valid - position;
		}
		if (cm.currentLength() <= 0) {
			return 0;
		}
		readInput(cm.currentLength());
		cm.incrementChunk();
		return valid;
	}

	@Override
	public void close() throws IOException {
		try {
			in.close();
		} finally {
			if (!closed) {
				closed = true;
			}
		}
	}

	private boolean ensureBuffer() throws IOException {
		if (available() > 0) {
			return true;
		}
		if (eof) {
			return false;
		}

		return false;
	}

	@Override
	public int read() throws IOException {
		if (closed) {
			return -1;
		}
		if (!ensureBuffer()) {
			return -1;
		}
		return buffer[position++] & 0xFF;
	}

	@Override
	public int read(byte[] output, int offset, int length) throws IOException {
		if (output == null) {
			throw new IOException("output is null");
		}
		if (closed) {
			throw new IOException("Stream is closed");
		}

		if (length == 0) {
			return 0;
		}
		if (!ensureBuffer()) {
			return -1;
		}

		int size = min(length, available());
		System.arraycopy(buffer, position, output, offset, size);
		position += size;
		return size;
	}

	private void readInput(int length) throws IOException {
		int offset = 0;
		while (offset < length) {
			int size = in.read(input, offset, length - offset);
			if (size == -1) {
				throw new EOFException("encountered EOF while reading block data");
			}
			offset += size;
		}
		// ignore checksum for now
		byte[] checksum = new byte[4];
		int size = in.read(checksum);

		if (size != 4) {
			throw new EOFException("encountered EOF while reading checksum");
		}

		valid = cm.compressor().uncompress(input, 0, length, buffer, 0);
		position = 0;
	}

}