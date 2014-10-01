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

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordReader;

import com.netflix.aegisthus.io.writable.AtomWritable;
import com.netflix.aegisthus.io.writable.CompositeKey;

public abstract class AegisthusRecordReader extends RecordReader<CompositeKey, AtomWritable> {
    protected long start;
    protected long end;
    protected long pos;
    protected CompositeKey key = null;
    protected AtomWritable value = null;

    @Override
    public void close() throws IOException {
    }

    @Override
    public CompositeKey getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public AtomWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

}
