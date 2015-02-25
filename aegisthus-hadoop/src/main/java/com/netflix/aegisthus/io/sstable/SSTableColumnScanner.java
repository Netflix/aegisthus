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

import com.netflix.aegisthus.io.writable.AtomWritable;
import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.ColumnSerializer.CorruptColumnException;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable.OnSubscribe;
import rx.Subscriber;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SSTableColumnScanner {
    private static final Logger LOG = LoggerFactory.getLogger(SSTableColumnScanner.class);
    private final long end;
    private final OnDiskAtom.Serializer serializer = OnDiskAtom.Serializer.instance;
    private final long start;
    private DataInputStream input;
    private long pos;
    private Descriptor.Version version = null;

    public SSTableColumnScanner(InputStream is, long start, long end, Descriptor.Version version) throws IOException {
        this.version = version;
        this.start = start;
        this.end = end;
        this.input = new DataInputStream(is);
        if (this.start > 0) {
            LOG.info("skipping to start: {}", start);
            skipUnsafe(start);
        }
        this.pos = start;
    }

    public void close() {
        if (input != null) {
            try {
                input.close();
            } catch (IOException e) {
                // ignore
            }
            input = null;
        }
    }

    void deserialize(Subscriber<? super AtomWritable> subscriber) {
        LOG.debug("current pos({}) done ({})", pos, hasMore() ? "has more" : "no more");
        while (hasMore()) {
            int keysize = -1;
            byte[] rowKey = null;
            try {
                keysize = input.readUnsignedShort();
                long rowSize = 2;
                rowKey = new byte[keysize];
                input.readFully(rowKey);
                rowSize += keysize;

                if (version.hasRowSizeAndColumnCount) {
                    rowSize += input.readLong() + 8;
                    // Since we have the row size in this version we can go ahead and set pos to the end of the row.
                    this.pos += rowSize;
                }

                /*
                 * The local deletion times are similar to the times that they
                 * were marked for delete, but we only care to know that it was
                 * deleted at all, so we will go with the long value as the
                 * timestamps for update are long as well.
                 */
                @SuppressWarnings({ "unused", "UnusedAssignment" })
                int localDeletionTime = input.readInt();
                rowSize += 4;
                long markedForDeleteAt = input.readLong();
                rowSize += 8;
                int columnCount = Integer.MAX_VALUE;
                if (version.hasRowSizeAndColumnCount) {
                    columnCount = input.readInt();
                }

                try {
                    rowSize += deserializeColumns(subscriber, rowKey, markedForDeleteAt, columnCount, input);
                } catch (OutOfMemoryError e) {
                    String message = "Out of memory while reading row for key " + keyToString(rowKey)
                            + "this may be caused by a corrupt file";
                    subscriber.onError(new IOException(message, e));
                } catch (CorruptColumnException e) {
                    String message = "Error in row for key " + keyToString(rowKey);
                    subscriber.onError(new IOException(message, e));
                }

                // For versions without row size we need to load the columns to figure out the size they occupy
                if (!version.hasRowSizeAndColumnCount) {
                    this.pos += rowSize;
                }
            } catch (OutOfMemoryError e) {
                String message = "Out of memory while reading row wth size " + keysize
                        + " for key " + keyToString(rowKey) + "this may be caused by a corrupt file";
                subscriber.onError(new IOException(message, e));
                break;
            } catch (IOException e) {
                subscriber.onError(e);
                break;
            }
        }
    }

    private String keyToString(byte[] rowKey) {
        if (rowKey == null) {
            return "null";
        }

        String str = BytesType.instance.getString(ByteBuffer.wrap(rowKey));
        return StringUtils.left(str, 32);
    }

    long deserializeColumns(Subscriber<? super AtomWritable> subscriber, byte[] rowKey, long deletedAt,
            int count,
            DataInput columns) throws IOException {
        long columnSize = 0;
        int actualColumnCount = 0;
        for (int i = 0; i < count; i++, actualColumnCount++) {
            // serialize columns
            OnDiskAtom atom = serializer.deserializeFromSSTable(
                    columns, ColumnSerializer.Flag.PRESERVE_SIZE, Integer.MIN_VALUE, version
            );
            if (atom == null) {
                // If atom was null that means this was a version that does not have version.hasRowSizeAndColumnCount
                // So we have to add the size for the end of row marker also
                columnSize += 2;
                break;
            }
            columnSize += atom.serializedSizeForSSTable();
            subscriber.onNext(AtomWritable.createWritable(rowKey, deletedAt, atom));
        }

        // This is a row with no columns, we still create a writable because we want to preserve this information
        if (actualColumnCount == 0) {
            subscriber.onNext(AtomWritable.createWritable(rowKey, deletedAt, null));
        }

        return columnSize;
    }

    @Override
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }

    public long getEnd() {
        return end;
    }

    public long getPos() {
        return pos;
    }

    public long getStart() {
        return start;
    }

    boolean hasMore() {
        return pos < end;
    }

    public rx.Observable<AtomWritable> observable() {
        final ExecutorService service = Executors.newSingleThreadExecutor();
        rx.Observable<AtomWritable> ret = rx.Observable.create(new OnSubscribe<AtomWritable>() {
            @Override
            public void call(final Subscriber<? super AtomWritable> subscriber) {
                service.execute(new Runnable() {
                    @Override
                    public void run() {
                        deserialize(subscriber);
                        subscriber.onCompleted();
                    }
                });
            }
        });
        LOG.info("created observable");
        return ret;
    }

    void skipUnsafe(long bytes) throws IOException {
        if (bytes <= 0) {
            return;
        }

        FileUtils.skipBytesFully(input, bytes);
    }
}
