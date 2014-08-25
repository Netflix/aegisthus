package com.netflix.aegisthus.io.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.io.IColumnSerializer.Flag;
import org.apache.cassandra.io.sstable.Descriptor.Version;
import org.apache.hadoop.io.Writable;

public class AtomWritable implements Writable {
    private final OnDiskAtom.Serializer serializer = new OnDiskAtom.Serializer(new ColumnSerializer());
    private OnDiskAtom atom;
    private long deletedAt;
    private byte[] key;

    public AtomWritable() {
    }

    public AtomWritable(byte[] key, long deletedAt, OnDiskAtom atom) throws IOException {
        this.atom = atom;
        this.deletedAt = deletedAt;
        this.key = key;
    }

    @Override
    public void readFields(DataInput dis) throws IOException {
        int length = dis.readInt();
        byte[] bytes = new byte[length];
        dis.readFully(bytes);
        this.key = bytes;
        this.deletedAt = dis.readLong();
        this.atom = serializer.deserializeFromSSTable(dis, Flag.PRESERVE_SIZE, Integer.MIN_VALUE, Version.CURRENT);
    }

    @Override
    public void write(DataOutput dos) throws IOException {
        dos.writeInt(this.key.length);
        dos.write(this.key);
        dos.writeLong(this.deletedAt);
        serializer.serializeForSSTable(this.atom, dos);
    }

    public OnDiskAtom getAtom() {
        return atom;
    }

    public long getDeletedAt() {
        return deletedAt;
    }

    public byte[] getKey() {
        return key;
    }
}