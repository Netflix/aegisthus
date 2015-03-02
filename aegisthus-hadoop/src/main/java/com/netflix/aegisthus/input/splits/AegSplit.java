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
package com.netflix.aegisthus.input.splits;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;

public class AegSplit extends InputSplit implements Writable {
    private static final Logger LOG = LoggerFactory.getLogger(AegSplit.class);

    protected long end;
    protected String[] hosts;
    protected Path path;
    protected long start;

    public static AegSplit createSplit(@Nonnull Path path, long start, long length, @Nonnull String[] hosts) {
        AegSplit split = new AegSplit();
        split.path = path;
        split.start = start;
        split.end = length + start;
        split.hosts = hosts;
        LOG.info("start: {}, end: {}", start, split.end);

        return split;
    }

    public long getDataEnd() {
        return end;
    }

    public long getEnd() {
        return end;
    }

    @Nonnull
    public InputStream getInput(@Nonnull Configuration conf) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream fileIn = fs.open(path);
        return new DataInputStream(new BufferedInputStream(fileIn));
    }

    @Override
    public long getLength() {
        return end - start;
    }

    @Override
    @Nonnull
    public String[] getLocations() throws IOException, InterruptedException {
        if (hosts == null) {
            throw new IllegalStateException("hosts should not be null at this point");
        }

        return hosts;
    }

    @Nonnull
    public Path getPath() {
        if (path == null) {
            throw new IllegalStateException("path should not be null at this point");
        }

        return path;
    }

    public long getStart() {
        return start;
    }

    @Override
    public void readFields(@Nonnull DataInput in) throws IOException {
        end = in.readLong();
        hosts = WritableUtils.readStringArray(in);
        path = new Path(WritableUtils.readString(in));
        start = in.readLong();
    }

    @Override
    public void write(@Nonnull DataOutput out) throws IOException {
        out.writeLong(end);
        WritableUtils.writeStringArray(out, hosts);
        WritableUtils.writeString(out, path.toUri().toString());
        out.writeLong(start);
    }
}
