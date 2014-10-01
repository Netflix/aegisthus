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
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.ColumnSerializer.CorruptColumnException;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import rx.Observable.OnSubscribe;
import rx.Subscriber;

import com.netflix.aegisthus.io.writable.AtomWritable;

public class SSTableColumnScanner extends SSTableReader {
    private static final Log LOG = LogFactory.getLog(SSTableColumnScanner.class);
    private Descriptor.Version version = null;
    private final OnDiskAtom.Serializer serializer = new OnDiskAtom.Serializer(new ColumnSerializer());

    public SSTableColumnScanner(Descriptor.Version version) {
        this.version = version;
        this.end = -1;
    }

    public SSTableColumnScanner(long end, Descriptor.Version version) {
        this.version = version;
        this.end = end;
    }

    public SSTableColumnScanner(InputStream is, Descriptor.Version version) {
        this.version = version;
        this.end = -1;
        init(is);
    }

    public SSTableColumnScanner(InputStream is, long end, Descriptor.Version version) {
        this.version = version;
        this.end = end;
        init(is);
    }

    protected void init(InputStream is) {
        this.is = is;
        this.input = new DataInputStream(is);
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
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }

    protected void deserialize(Subscriber<? super AtomWritable> subscriber) {
        LOG.info(String.format("current pos(%d) done (%s)", pos, hasMore() ? "has more" : "no more"));
        while (hasMore()) {
            try {
                checkPosition(subscriber);
                int keysize = input.readUnsignedShort();
                byte[] rowKey = new byte[keysize];
                input.readFully(rowKey);
                datasize = input.readLong() + keysize + 2 + 8;
                /*
                 * The indexScanner is here to check to make sure that we are at
                 * the correct place in the file.
                 */
                String hexKey = BytesType.instance.getString(ByteBuffer.wrap(rowKey));
                if (!validateRow(subscriber, hexKey, datasize) || (end != -1 && this.pos + datasize > end)) {
                    continue;
                }
                this.pos += datasize;
                int bfsize = 0;
                int idxsize = 0;
                if (!version.hasPromotedIndexes) {
                    if (input instanceof DataInputStream) {
                        // skip bloom filter
                        bfsize = input.readInt();
                        skip(bfsize);
                        // skip index
                        idxsize = input.readInt();
                        skip(idxsize);
                    } else {
                        // skip bloom filter
                        bfsize = input.readInt();
                        input.skipBytes(bfsize);
                        // skip index
                        idxsize = input.readInt();
                        input.skipBytes(idxsize);
                    }
                }
                /*
                 * The local deletion times are similar to the times that they
                 * were marked for delete, but we only care to know that it was
                 * deleted at all, so we will go with the long value as the
                 * timestamps for update are long as well.
                 */
                @SuppressWarnings("unused")
                int localDeletionTime = input.readInt();
                long markedForDeleteAt = input.readLong();
                int columnCount = input.readInt();
                try {
                    serializeColumns(subscriber, rowKey, markedForDeleteAt, columnCount, input);
                } catch (CorruptColumnException e) {
                    // TODO: new exception that has row key
                    subscriber.onError(new IOException(hexKey, e));
                }
            } catch (IOException e) {
                subscriber.onError(e);
                break;
            }
        }
    }

    protected void serializeColumns(Subscriber<? super AtomWritable> subscriber, byte[] rowKey, long deletedAt, int count,
            DataInput columns) throws IOException {
        for (int i = 0; i < count; i++) {
            // serialize columns
            OnDiskAtom atom = serializer.deserializeFromSSTable(columns, version);
            subscriber.onNext(new AtomWritable(rowKey, deletedAt, atom));
        }
    }

    /**
     * Here so that it can be overridden by a scanner that keeps track of position
     */
    protected boolean validateRow(Subscriber<? super AtomWritable> subscriber, String key, long datasize) {
        return true;
    }

    /**
     * Here so that it can be overridden by a scanner that keeps track of position
     */
    protected void checkPosition(Subscriber<? super AtomWritable> subscriber) throws IOException {
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
}
