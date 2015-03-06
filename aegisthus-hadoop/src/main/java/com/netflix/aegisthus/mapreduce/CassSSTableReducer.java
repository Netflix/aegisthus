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
package com.netflix.aegisthus.mapreduce;

import com.google.common.collect.Lists;
import com.netflix.Aegisthus;
import com.netflix.aegisthus.io.writable.AegisthusKey;
import com.netflix.aegisthus.io.writable.AtomWritable;
import com.netflix.aegisthus.io.writable.RowWritable;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

public class CassSSTableReducer extends Reducer<AegisthusKey, AtomWritable, BytesWritable, RowWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(CassSSTableReducer.class);
    private long rowsToAddToCounter = 0;
    private AbstractType<?> columnComparator;
    private AbstractType<?> rowKeyComparator;
    private long maxRowSize = Long.MAX_VALUE;

    @Override protected void setup(
            Context context)
            throws IOException, InterruptedException {
        super.setup(context);

        maxRowSize = context.getConfiguration().getLong(Aegisthus.Feature.CONF_MAXCOLSIZE, Long.MAX_VALUE);

        String columnType = context.getConfiguration().get(Aegisthus.Feature.CONF_COLUMNTYPE, "BytesType");
        String rowKeyType = context.getConfiguration().get(Aegisthus.Feature.CONF_KEYTYPE, "BytesType");

        try {
            columnComparator = TypeParser.parse(columnType);
            rowKeyComparator = TypeParser.parse(rowKeyType);
        } catch (SyntaxException | ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        updateCounter(context, true);
        super.cleanup(context);
    }

    @Override
    public void reduce(AegisthusKey key, Iterable<AtomWritable> values, Context ctx)
            throws IOException, InterruptedException {
        RowReducer rowReducer = new RowReducer(columnComparator);

        for (AtomWritable value : values) {
            if (rowReducer.key == null) {
                rowReducer.key = value.getKey();

                if (LOG.isDebugEnabled()) {
                    String formattedKey = rowKeyComparator.getString(ByteBuffer.wrap(rowReducer.key));
                    LOG.debug("Doing reduce for key '{}'", formattedKey);
                }
            }

            if (value.getDeletedAt() > rowReducer.deletedAt) {
                rowReducer.deletedAt = value.getDeletedAt();
            }

            if (value.getAtom() != null &&
                    rowReducer.getAtomTotalSize() + value.getAtom().serializedSizeForSSTable() > maxRowSize) {
                String formattedKey = rowKeyComparator.getString(ByteBuffer.wrap(rowReducer.key));
                LOG.warn("Skipping part of row {} that is too big, current size is already {}.",
                        formattedKey, rowReducer.getAtomTotalSize());
                ctx.getCounter("aegisthus", "reducerRowsTooBig").increment(1L);
                break;
            }

            rowReducer.addAtom(value);
        }

        rowReducer.finalizeReduce();

        BytesWritable bytesWritable = new BytesWritable(key.getKey().array());
        ctx.write(bytesWritable, RowWritable.createRowWritable(rowReducer.columns, rowReducer.deletedAt));
        updateCounter(ctx, false);
    }

    void updateCounter(Context ctx, boolean flushRegardlessOfCount) {
        if (flushRegardlessOfCount && rowsToAddToCounter != 0) {
            ctx.getCounter("aegisthus", "rows_written").increment(rowsToAddToCounter);
            rowsToAddToCounter = 0;
        } else {
            if (rowsToAddToCounter % 100 == 0) {
                ctx.getCounter("aegisthus", "rows_written").increment(rowsToAddToCounter);
                rowsToAddToCounter = 0;
            }
            rowsToAddToCounter++;
        }
    }

    static class RowReducer {
        private final List<OnDiskAtom> columns = Lists.newArrayList();
        // TODO: need to get comparator
        private final RangeTombstone.Tracker tombstoneTracker;
        private OnDiskAtom currentColumn = null;
        private long deletedAt = Long.MIN_VALUE;
        private byte[] key;
        private long atomTotalSize = 0L;

        RowReducer(AbstractType<?> columnComparator) {
            tombstoneTracker = new RangeTombstone.Tracker(columnComparator);
        }

        @SuppressWarnings("StatementWithEmptyBody")
        public void addAtom(AtomWritable writable) {
            OnDiskAtom atom = writable.getAtom();
            if (atom == null) {
                return;
            }

            atomTotalSize += atom.serializedSizeForSSTable();

            this.tombstoneTracker.update(atom);
            // Right now, we will only keep columns. This works because we will
            // have all the columns a range tombstone applies to when we create
            // a snapshot. This will not be true if we go to partial incremental
            // processing
            if (atom instanceof Column) {
                Column column = (Column) atom;
                if (this.tombstoneTracker.isDeleted(column)) {
                    // If the column is deleted by the rangeTombstone, just discard
                    // it, every other column of the same name will be discarded as
                    // well, unless it is later than the range tombstone in which
                    // case the column is out of date anyway
                } else if (currentColumn == null) {
                    currentColumn = column;
                } else if (currentColumn.name().equals(column.name())) {
                    if (column.timestamp() > currentColumn.minTimestamp()) {
                        currentColumn = column;
                    }
                } else {
                    columns.add(currentColumn);
                    currentColumn = column;
                }
            } else if (atom instanceof RangeTombstone) {
                // We do not include these columns in the output since they are deleted
            } else {
                String error =
                        "Cassandra added a new type " + atom.getClass().getCanonicalName() + " which we do not support";
                throw new IllegalArgumentException(error);
            }
        }

        public void finalizeReduce() {
            if (currentColumn != null) {
                columns.add(currentColumn);
            }

            // When cassandra compacts it removes columns that are in deleted rows
            // that are older than the deleted timestamp.
            // we will duplicate this behavior. If the etl needs this data at some
            // point we can change, but it is only available assuming
            // cassandra hasn't discarded it.
            Iterator<OnDiskAtom> columnIterator = columns.iterator();
            while (columnIterator.hasNext()) {
                OnDiskAtom atom = columnIterator.next();
                if (atom instanceof RangeTombstone) {
                    columnIterator.remove();
                } else if (atom instanceof Column && ((Column) atom).timestamp() <= this.deletedAt) {
                    columnIterator.remove();
                }
            }
        }

        public long getAtomTotalSize() {
            return atomTotalSize;
        }
    }
}
