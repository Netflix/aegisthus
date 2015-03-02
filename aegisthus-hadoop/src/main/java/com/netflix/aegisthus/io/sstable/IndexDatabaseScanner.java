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

import org.apache.cassandra.io.util.FileUtils;
import org.apache.commons.io.input.CountingInputStream;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 * This class reads an SSTable index file and returns the offset for each key.
 */
public class IndexDatabaseScanner implements Iterator<IndexDatabaseScanner.OffsetInfo>, Closeable {
    private final CountingInputStream countingInputStream;
    private final DataInputStream input;

    public IndexDatabaseScanner(@Nonnull InputStream is) {
        this.countingInputStream = new CountingInputStream(is);
        this.input = new DataInputStream(this.countingInputStream);
    }

    @Override
    public void close() {
        try {
            input.close();
        } catch (IOException ignored) {
        }
    }

    @Override
    public boolean hasNext() {
        try {
            return input.available() != 0;
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    @Override
    @Nonnull
    public OffsetInfo next() {
        try {
            long indexOffset = countingInputStream.getByteCount();
            int keysize = input.readUnsignedShort();
            input.skipBytes(keysize);
            Long dataOffset = input.readLong();
            skipPromotedIndexes();
            return new OffsetInfo(dataOffset, indexOffset);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    void skipPromotedIndexes() throws IOException {
        int size = input.readInt();
        if (size <= 0) {
            return;
        }

        FileUtils.skipBytesFully(input, size);
    }

    public static class OffsetInfo {
        private final long dataFileOffset;
        private final long indexFileOffset;

        public OffsetInfo(long dataFileOffset, long indexFileOffset) {
            this.dataFileOffset = dataFileOffset;
            this.indexFileOffset = indexFileOffset;
        }

        public long getDataFileOffset() {
            return dataFileOffset;
        }

        @SuppressWarnings("unused")
        public long getIndexFileOffset() {
            return indexFileOffset;
        }
    }
}
