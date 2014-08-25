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
import java.util.List;

import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.IColumn;
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
    public static class Reduce {
        private List<OnDiskAtom> columns;
        private IColumn currentColumn = null;
        private Long deletedAt = Long.MIN_VALUE;
        private byte[] key = null;
        private final OnDiskAtom.Serializer serializer = new OnDiskAtom.Serializer(new ColumnSerializer());
        private RangeTombstone.Tracker tombstoneTracker;

        public Reduce() {
            columns = Lists.newArrayList();
            // TODO: need to get comparator
            this.tombstoneTracker = new RangeTombstone.Tracker(BytesType.instance);
        }

        public void addAtom(OnDiskAtom atom) {
            if (atom == null) {
                return;
            }
            this.tombstoneTracker.update(atom);
            // Right now, we will only keep columns. This works because we will
            // have all the columns a range tombstone applies to when we create
            // a snapshot. This will not be true if we go to partial incremental
            // processing
            if (atom instanceof IColumn) {
                IColumn column = (IColumn) atom;
                // If the column is deleted by the rangeTombstone, just discard
                // it, every other column of the same name will be discarded as
                // well, unless it is later than the range tombstone in which
                // case the column is out of date anyway
                if (this.tombstoneTracker.isDeleted(column)) {
                    return;
                }
                if (currentColumn == null) {
                    currentColumn = column;
                    return;
                } else if (currentColumn.name().equals(column.name())) {
                    if (column.timestamp() > currentColumn.timestamp()) {
                        currentColumn = column;
                    }
                } else {
                    columns.add(currentColumn);
                    currentColumn = column;
                }
            }
        }
        
        //TODO: expire columns
        public void finalize() {
            if (currentColumn != null) {
                columns.add(currentColumn);
            }
        }

        public void setDeletedAt(long deletedAt) {
            this.deletedAt = deletedAt;
        }

        public void setKey(byte[] key) {
            this.key = key;
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
    }

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
        boolean first = true;
        for (AtomWritable value : values) {
            if (first) {
                reduce.setKey(value.getKey());
                reduce.setDeletedAt(value.getDeletedAt());
            }
            reduce.addAtom(value.getAtom());
        }
        reduce.finalize();
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
}
