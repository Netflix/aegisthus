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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.netflix.aegisthus.columnar_input.splits.AegIndexedSplit;
import com.netflix.aegisthus.columnar_input.splits.AegSplit;
import com.netflix.aegisthus.io.sstable.IndexedSSTableScanner;
import com.netflix.aegisthus.io.sstable.SSTableColumnScanner;
import com.netflix.aegisthus.io.writable.AtomWritable;
import com.netflix.aegisthus.io.writable.CompositeKey;

public class SSTableRecordReader extends AegisthusRecordReader {
    private static final Log LOG = LogFactory.getLog(SSTableRecordReader.class);
    private SSTableColumnScanner scanner;
    private Iterator<AtomWritable> iterator = null;

    @Override
    public void close() throws IOException {
        super.close();
        if (scanner != null) {
            scanner.close();
        }
    }

    @Override
    public void initialize(InputSplit inputSplit, final TaskAttemptContext ctx) throws IOException,
            InterruptedException {
        AegSplit split = (AegSplit) inputSplit;

        start = split.getStart();
        InputStream is = split.getInput(ctx.getConfiguration());
        end = split.getDataEnd();
        String filename = split.getPath().toUri().toString();

        LOG.info(String.format("File: %s", split.getPath().toUri().getPath()));
        LOG.info("Start: " + start);
        LOG.info("End: " + end);

        try {
            DataInput indexInput;
            if (inputSplit instanceof AegIndexedSplit) {
                AegIndexedSplit indexedSplit = (AegIndexedSplit) inputSplit;
                indexInput = new DataInputStream(indexedSplit.getIndexInput(ctx.getConfiguration()));
                scanner = new IndexedSSTableScanner(is, end, Descriptor.fromFilename(filename).version, indexInput);
            } else {
                scanner = new SSTableColumnScanner(is, end, Descriptor.fromFilename(filename).version);
            }
            LOG.info("skipping to start: " + start);
            scanner.skipUnsafe(start);
            this.pos = start;
            LOG.info("Creating observable");
            iterator = scanner.observable()
                    //TODO: This code should be included when we want to add skipping error rows.
            /*.onErrorFlatMap(new Func1<OnErrorThrowable, Observable<? extends Column>>() {
                @Override
                public Observable<? extends Column> call(OnErrorThrowable onErrorThrowable) {
                    LOG.error("failure deserializing", onErrorThrowable);
                    if (ctx instanceof TaskInputOutputContext) {
                        ((TaskInputOutputContext) ctx).getCounter("aegisthus",
                                onErrorThrowable.getCause().getClass().getSimpleName()).increment(1L);
                    }
                    return Observable.empty();
                }
            })*/
                    .toBlocking()
                    .toIterable()
                    .iterator();
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
