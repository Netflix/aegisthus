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

import com.netflix.aegisthus.input.splits.AegSplit;
import com.netflix.aegisthus.io.sstable.SSTableColumnScanner;
import com.netflix.aegisthus.io.writable.AegisthusKey;
import com.netflix.aegisthus.io.writable.AtomWritable;
import com.netflix.aegisthus.util.ObservableToIterator;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.exceptions.OnErrorThrowable;
import rx.functions.Func1;

import javax.annotation.Nonnull;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class SSTableRecordReader extends RecordReader<AegisthusKey, AtomWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(SSTableRecordReader.class);
    private Iterator<AtomWritable> iterator;
    private AegisthusKey key;
    private SSTableColumnScanner scanner;
    private AtomWritable value;

    @Override
    public void close() throws IOException {
        if (scanner != null) {
            scanner.close();
        }
    }

    @Override
    public AegisthusKey getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public AtomWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (scanner.getStart() == scanner.getEnd()) {
            return 0.0f;
        } else {
            long completed = scanner.getPos() - scanner.getStart();
            float total = scanner.getEnd() - scanner.getStart();
            return Math.min(1.0f, completed / total);
        }
    }

    @Override
    public void initialize(@Nonnull InputSplit inputSplit, @Nonnull final TaskAttemptContext ctx)
            throws IOException, InterruptedException {
        final AegSplit split = (AegSplit) inputSplit;

        long start = split.getStart();
        InputStream is = split.getInput(ctx.getConfiguration());
        long end = split.getDataEnd();
        String filename = split.getPath().toUri().toString();

        LOG.info("File: {}", split.getPath().toUri().getPath());
        LOG.info("Start: {}", start);
        LOG.info("End: {}", end);

        try {
            scanner = new SSTableColumnScanner(is, start, end, Descriptor.fromFilename(filename).version);
            LOG.info("Creating observable");
            rx.Observable<AtomWritable> observable = scanner.observable();
            observable = observable
                    .onErrorFlatMap(new Func1<OnErrorThrowable, Observable<? extends AtomWritable>>() {
                        @Override
                        public Observable<? extends AtomWritable> call(OnErrorThrowable onErrorThrowable) {
                            LOG.error("failure deserializing file {}", split.getPath(), onErrorThrowable);
                            ctx.getCounter("aegisthus", "error_skipped_input").increment(1L);
                            return Observable.empty();
                        }
                    });

            iterator = ObservableToIterator.toIterator(observable);
            LOG.info("done initializing");
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!iterator.hasNext()) {
            return false;
        }

        value = iterator.next();
        if (value.getAtom() != null) {
            key = AegisthusKey.createKeyForRowColumnPair(
                    ByteBuffer.wrap(value.getKey()),
                    value.getAtom().name(),
                    value.getAtom().maxTimestamp()
            );
        } else {
            key = AegisthusKey.createKeyForRow(ByteBuffer.wrap(value.getKey()));
        }

        return true;
    }
}
