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
package com.netflix.aegisthus.input.readers;

import com.netflix.aegisthus.input.splits.AegCombinedSplit;
import com.netflix.aegisthus.io.writable.AegisthusKey;
import com.netflix.aegisthus.io.writable.AtomWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CombineSSTableReader extends RecordReader<AegisthusKey, AtomWritable> {
    private static final Logger log = LoggerFactory.getLogger(CombineSSTableReader.class);
    private TaskAttemptContext ctx;
    private AegCombinedSplit splits;
    private int currentSplitNumber;
    private int splitCount;
    private SSTableRecordReader reader;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext ctx) throws IOException, InterruptedException {
        this.ctx = ctx;
        splits = (AegCombinedSplit) split;
        currentSplitNumber = 0;
        splitCount = splits.getSplits().size();
        log.info("splits to process: {}", splitCount);
        reader = new SSTableRecordReader();
        reader.initialize(splits.getSplits().get(currentSplitNumber), this.ctx);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean resp = reader.nextKeyValue();
        if (resp) {
            return true;
        }

        reader.close();

        currentSplitNumber++;
        if (currentSplitNumber >= splits.getSplits().size()) {
            return false;
        }

        log.info("Switching to split number {}", currentSplitNumber);
        reader.initialize(splits.getSplits().get(currentSplitNumber), this.ctx);
        return reader.nextKeyValue();
    }

    @Override
    public AegisthusKey getCurrentKey() throws IOException, InterruptedException {
        return reader.getCurrentKey();
    }

    @Override
    public AtomWritable getCurrentValue() throws IOException, InterruptedException {
        return reader.getCurrentValue();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        float currentFileProgress = currentSplitNumber + reader.getProgress();
        return Math.min(1.0f, currentFileProgress / splitCount);
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }
}
