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
package com.netflix.aegisthus.columnar_input.readers;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import rx.Observable;
import rx.exceptions.OnErrorThrowable;
import rx.functions.Func1;

import com.netflix.aegisthus.columnar_input.splits.AegSplit;
import com.netflix.aegisthus.io.commitlog.CommitLogColumnarScanner;
import com.netflix.aegisthus.io.writable.AtomWritable;
import com.netflix.aegisthus.io.writable.CompositeKey;

public class CommitLogRecordReader extends AegisthusRecordReader {
    private static final Log LOG = LogFactory.getLog(CommitLogRecordReader.class);
    protected CommitLogColumnarScanner scanner;
    protected int cfId;
    private Iterator<AtomWritable> iterator = null;

    @Override
    public void close() throws IOException {
        super.close();
        if (scanner != null) {
            scanner.close();
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void initialize(InputSplit inputSplit, final TaskAttemptContext ctx) throws IOException,
            InterruptedException {
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
            scanner = new CommitLogColumnarScanner(new DataInputStream(dis),
                    Descriptor.fromFilename(split.getPath().getName()).version, cfId);
            this.pos = start;
            iterator = scanner.observable()
                    .onErrorFlatMap(new Func1<OnErrorThrowable, Observable<? extends AtomWritable>>() {
                        @Override
                        public Observable<? extends AtomWritable> call(OnErrorThrowable onErrorThrowable) {
                            LOG.error("failure deserializing", onErrorThrowable);
                            if (ctx instanceof TaskInputOutputContext) {
                                ctx.getCounter("aegisthus",
                                        onErrorThrowable.getCause().getClass().getSimpleName()).increment(1L);
                            }
                            return Observable.empty();
                        }
                    })
                    .toBlocking()
                    .toIterable()
                    .iterator();
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!iterator.hasNext()) {
            return false;
        }
        AtomWritable atomWritable = iterator.next();
        key = new CompositeKey(
                ByteBuffer.wrap(atomWritable.getKey()),
                atomWritable.getAtom().name(),
                atomWritable.getAtom().maxTimestamp()
        );
        value = atomWritable;
        return true;
    }

}
