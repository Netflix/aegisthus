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
package com.netflix.aegisthus.io.writable;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.netflix.Aegisthus;
import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.io.sstable.Descriptor.Version;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class RowWritable implements Writable, Configurable {
    private final OnDiskAtom.Serializer serializer = OnDiskAtom.Serializer.instance;
    private List<OnDiskAtom> columns;
    private Configuration configuration;
    private long deletedAt;
    private Version version;

    public static RowWritable createRowWritable(List<OnDiskAtom> columns, long deletedAt) {
        RowWritable rowWritable = new RowWritable();
        rowWritable.columns = columns;
        rowWritable.deletedAt = deletedAt;

        return rowWritable;
    }

    public List<OnDiskAtom> getColumns() {
        return columns;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    @Override
    public void setConf(Configuration conf) {
        this.configuration = conf;
        String sstableVersion = conf.get(Aegisthus.Feature.CONF_SSTABLE_VERSION);
        Preconditions.checkState(!Strings.isNullOrEmpty(sstableVersion), "SSTable version is required configuration");
        this.version = new Version(sstableVersion);
    }

    public long getDeletedAt() {
        return deletedAt;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        deletedAt = in.readLong();
        int columnCount = in.readInt();
        columns = Lists.newArrayListWithCapacity(columnCount);
        for (int i = 0; i < columnCount; i++) {
            OnDiskAtom atom = serializer.deserializeFromSSTable(
                    in, ColumnSerializer.Flag.PRESERVE_SIZE, Integer.MIN_VALUE, version
            );
            columns.add(atom);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(deletedAt);
        out.writeInt(columns.size());
        for (OnDiskAtom column : columns) {
            serializer.serializeForSSTable(column, out);
        }
    }
}
