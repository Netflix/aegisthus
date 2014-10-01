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
package com.netflix.aegisthus.pig;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadMetadata;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.netflix.aegisthus.input.AegisthusInputFormat;
import com.netflix.aegisthus.tools.AegisthusSerializer;

/**
 * Pig loader for sstable format.
 */
public class AegisthusColumnarLoader extends PigStorage implements LoadMetadata {
    private AegisthusSerializer serializer;
    private RecordReader rr;

    public AegisthusColumnarLoader() {
    }

    @SuppressWarnings("rawtypes")
    @Override
    public InputFormat getInputFormat() {
        return new AegisthusInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        rr = reader;
        super.prepareToRead(reader, split);
    }

    @Override
    public Tuple getNext() throws IOException {
        List<String> t = Lists.newArrayList();
        try {
            if (rr.nextKeyValue()) {
                t.add(rr.getCurrentKey().toString());
                t.add(rr.getCurrentValue().toString());
                return TupleFactory.getInstance().newTuple(t);
            }
        } catch (Throwable e) {
            Throwables.propagateIfPossible(e, IOException.class);
            throw Throwables.propagate(e);
        }
        return null;
    }
}
