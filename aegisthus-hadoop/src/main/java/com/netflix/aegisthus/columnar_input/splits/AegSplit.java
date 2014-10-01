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
package com.netflix.aegisthus.columnar_input.splits;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;

public class AegSplit extends InputSplit implements Writable {
    private static final Log LOG = LogFactory.getLog(AegSplit.class);

    public enum Type {
        commitlog, sstable
    }

    protected long end;
    protected String[] hosts;
    protected Path path;
    protected long start;
    protected Type type;

    public AegSplit() {
    }

    public AegSplit(Path path, long start, long length, String[] hosts) {
        this(path, start, length, hosts, Type.sstable);
    }

    public AegSplit(Path path, long start, long length, String[] hosts, Type type) {
        this.type = type;
        this.path = path;
        this.start = start;
        this.end = length + start;
        LOG.info(String.format("start: %d, end: %d", start, end));
        this.hosts = hosts;
    }

    public long getEnd() {
        return end;
    }

    public long getDataEnd() {
        return end;
    }

    @Override
    public long getLength() {
        return end - start;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return hosts;
    }

    public Path getPath() {
        return path;
    }

    public long getStart() {
        return start;
    }

    public Type getType() {
        return type;
    }

    public InputStream getInput(Configuration conf) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream fileIn = fs.open(path);
        return new DataInputStream(new BufferedInputStream(fileIn));
    }

    public InputStream getIndexInput(Configuration conf) throws IOException {
        return null;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        end = in.readLong();
        hosts = WritableUtils.readStringArray(in);
        path = new Path(WritableUtils.readString(in));
        start = in.readLong();
        type = WritableUtils.readEnum(in, Type.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(end);
        WritableUtils.writeStringArray(out, hosts);
        WritableUtils.writeString(out, path.toUri().toString());
        out.writeLong(start);
        WritableUtils.writeEnum(out, type);
    }

}
