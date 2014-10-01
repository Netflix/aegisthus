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
package com.netflix.aegisthus.mapred.reduce;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.sstable.Descriptor.Version;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.netflix.aegisthus.io.writable.AtomWritable;
import com.netflix.aegisthus.io.writable.CompositeKey;
import com.netflix.aegisthus.tools.StorageHelper;

public class CassSSTableReducer extends Reducer<CompositeKey, AtomWritable, Text, Text> {
    private static final Logger LOG = LoggerFactory.getLogger(CassSSTableReducer.class);
    FSDataOutputStream dos;
    Reduce reduce;

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        dos.close();
        super.cleanup(context);
    }

    @Override
    public void reduce(CompositeKey key, Iterable<AtomWritable> values, Context ctx) throws IOException, InterruptedException {
        reduce = new Reduce();

        for (AtomWritable value : values) {
            reduce.setKey(value.getKey());
            if (value.getDeletedAt() > reduce.getDeletedAt()) {
                reduce.setDeletedAt(value.getDeletedAt());
            }
            reduce.addAtom(value.getAtom());
        }
        reduce.finalizeReduce();
        reduce.writeRow(dos);
        // TODO: batch this
        ctx.getCounter("aegisthus", "rows_written").increment(1L);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        StorageHelper sh = new StorageHelper(context);
        Path outputDir = new Path(sh.getBaseTaskAttemptTempLocation());
        FileSystem fs = outputDir.getFileSystem(context.getConfiguration());
        String filename = String.format("%s-%s-%05d0%04d-Data.db",
                context.getConfiguration().get("aegisthus.dataset", "keyspace-dataset"), Version.current_version,
                context.getTaskAttemptID().getTaskID().getId(), context.getTaskAttemptID().getId());
        Path outputFile = new Path(outputDir, filename);
        LOG.info("writing to: {}", outputFile.toUri());
        dos = fs.create(outputFile);
        super.setup(context);
    }

    static class Reduce {
        private final List<OnDiskAtom> columns = Lists.newArrayList();
        private final OnDiskAtom.Serializer serializer = new OnDiskAtom.Serializer(new ColumnSerializer());
        // TODO: need to get comparator
        private final RangeTombstone.Tracker tombstoneTracker = new RangeTombstone.Tracker(BytesType.instance);
        private OnDiskAtom currentColumn = null;
        private long deletedAt = Long.MIN_VALUE;
        private byte[] key;

        public void setKey(byte[] key) {
            this.key = key;
        }

        @SuppressWarnings("StatementWithEmptyBody")
        public void addAtom(OnDiskAtom atom) {
            if (atom == null) {
                return;
            }
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
                throw new IllegalArgumentException("Cassandra added a new type " + atom.getClass().getCanonicalName() + " which we do not support");
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

        public void writeRow(DataOutput dos) throws IOException {
            dos.writeShort(this.key.length);
            dos.write(this.key);
            long dataSize = 16; // The bytes for the Int, Long, Int after this loop
            for (OnDiskAtom atom : columns) {
                dataSize += atom.serializedSizeForSSTable();
            }
            dos.writeLong(dataSize);
            dos.writeInt((int) (this.deletedAt / 1000));
            dos.writeLong(this.deletedAt);
            dos.writeInt(columns.size());
            for (OnDiskAtom atom : columns) {
                serializer.serializeForSSTable(atom, dos);
            }

        }

        public long getDeletedAt() {
            return deletedAt;
        }

        public void setDeletedAt(long deletedAt) {
            this.deletedAt = deletedAt;
        }
    }
}
