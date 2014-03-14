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
package com.netflix.aegisthus.input.readers;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.netflix.aegisthus.input.AegSplit;
import com.netflix.aegisthus.io.commitlog.CommitLogScanner;

public class CommitLogRecordReader extends AegisthusRecordReader {
    protected CommitLogScanner scanner;
    protected int cfId;

    @Override
    public void close() throws IOException {
        super.close();
        if (scanner != null) {
            scanner.close();
        }
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext ctx) throws IOException, InterruptedException {
        AegSplit split = (AegSplit) inputSplit;

        start = split.getStart();
        end = split.getEnd();
        final Path file = split.getPath();

        try {
            cfId = ctx.getConfiguration().getInt("commitlog.cfid", -1000);
            if (cfId == -1000) {
                throw new IOException("commitlog.cfid must be set");
            }
            // open the file and seek to the start of the split
            FileSystem fs = file.getFileSystem(ctx.getConfiguration());
            FSDataInputStream fileIn = fs.open(split.getPath());
            InputStream dis = new BufferedInputStream(fileIn);
            scanner = new CommitLogScanner(new DataInputStream(dis), split.getConvertors(),
                    Descriptor.fromFilename(split.getPath().getName()).version);
            this.pos = start;
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        String line = scanner.next();
        if (line == null) {
            return false;
        }
        pos += scanner.getDatasize();
        String[] text = line.split("\t", 2);
        if (text[0].charAt(0) == '{') {
            text[0] = text[0].substring(2, text[0].length() - 1);
        }
        key.set(text[0]);
        value.set(text[1]);
        return true;
    }

}
