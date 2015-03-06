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
import java.util.Objects;

public class AtomWritable implements Writable, Configurable {
    private final OnDiskAtom.Serializer serializer = OnDiskAtom.Serializer.instance;
    private OnDiskAtom atom;
    private Configuration configuration;
    private long deletedAt;
    private byte[] key;
    private Version version;

    /**
     * Constructs a new AtomWritable for the row/column pair
     *
     * @param key the row key
     * @param deletedAt when the row was deleted, Long.MAX_VALUE if not deleted
     * @param atom the on disk representation of this row/column pair
     *
     * @return a new AtomWritable representing the given row/column pair.
     */
    public static AtomWritable createWritable(byte[] key, long deletedAt, OnDiskAtom atom) {
        AtomWritable writable = new AtomWritable();
        writable.key = key;
        writable.deletedAt = deletedAt;
        writable.atom = atom;

        return writable;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {return true;}
        if (obj == null || getClass() != obj.getClass()) {return false;}
        final AtomWritable other = (AtomWritable) obj;
        return Objects.equals(this.serializer, other.serializer)
                && Objects.equals(this.atom, other.atom)
                && Objects.equals(this.deletedAt, other.deletedAt)
                && Objects.equals(this.key, other.key);
    }

    public OnDiskAtom getAtom() {
        return atom;
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

    public byte[] getKey() {
        return key;
    }

    @Override
    public int hashCode() {
        return Objects.hash(serializer, atom, deletedAt, key);
    }

    @Override
    public void readFields(DataInput dis) throws IOException {
        int length = dis.readInt();
        byte[] bytes = new byte[length];
        dis.readFully(bytes);
        this.key = bytes;
        this.deletedAt = dis.readLong();

        boolean hasAtom = dis.readBoolean();
        if (hasAtom) {
            this.atom = serializer.deserializeFromSSTable(
                    dis, ColumnSerializer.Flag.PRESERVE_SIZE, Integer.MIN_VALUE, version
            );
        } else {
            this.atom = null;
        }
    }

    @Override
    public String toString() {
        return com.google.common.base.Objects.toStringHelper(this)
                .add("key", key)
                .add("deletedAt", deletedAt)
                .toString();
    }

    @Override
    public void write(DataOutput dos) throws IOException {
        dos.writeInt(this.key.length);
        dos.write(this.key);
        dos.writeLong(this.deletedAt);

        if (this.atom != null) {
            dos.writeBoolean(true);
            serializer.serializeForSSTable(this.atom, dos);
        } else {
            dos.writeBoolean(false);
        }
    }
}
