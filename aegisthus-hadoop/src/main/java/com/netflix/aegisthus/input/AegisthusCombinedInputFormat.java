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
package com.netflix.aegisthus.input;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.Aegisthus;
import com.netflix.aegisthus.input.readers.CombineSSTableReader;
import com.netflix.aegisthus.input.splits.AegCombinedSplit;
import com.netflix.aegisthus.input.splits.AegSplit;
import com.netflix.aegisthus.io.writable.AegisthusKey;
import com.netflix.aegisthus.io.writable.AtomWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * This class takes the splits created by the AegisthusInputFormat and combines
 * small SSTables into single splits.
 */
public class AegisthusCombinedInputFormat extends AegisthusInputFormat {
    private static final Logger log = LoggerFactory.getLogger(AegisthusCombinedInputFormat.class);

    @Override
    public RecordReader<AegisthusKey, AtomWritable> createRecordReader(InputSplit inputSplit,
            TaskAttemptContext context) throws IOException, InterruptedException {
        if (AegCombinedSplit.class.isAssignableFrom(inputSplit.getClass())) {
            return new CombineSSTableReader();
        } else {
            return super.createRecordReader(inputSplit, context);
        }
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        String tmp = job.getConfiguration().getTrimmed(Aegisthus.Feature.CONF_BLOCKSIZE, "104857600");
        long maxSplitSize = Long.valueOf(tmp);
        tmp = job.getConfiguration().getTrimmed(Aegisthus.Feature.CONF_MAX_COMBINED_SPLITS, "200");
        int maxSplitCount = Integer.valueOf(tmp);

        List<InputSplit> splits = super.getSplits(job);
        List<InputSplit> combinedSplits = Lists.newArrayList();
        Map<String, AegCombinedSplit> map = Maps.newHashMap();

        top:
        for (InputSplit split : splits) {
            AegSplit aegSplit = (AegSplit) split;
            if (aegSplit.getLength() >= maxSplitSize) {
                combinedSplits.add(aegSplit);
                continue;
            }
            try {
                String lastLocation = null;
                for (String location : aegSplit.getLocations()) {
                    lastLocation = location;
                    if (map.containsKey(location)) {
                        AegCombinedSplit temp = map.get(location);
                        temp.addSplit(aegSplit);
                        if (temp.getLength() >= maxSplitSize || temp.getSplits().size() >= maxSplitCount) {
                            combinedSplits.add(temp);
                            map.remove(location);
                        }
                        continue top;
                    }
                }
                AegCombinedSplit temp = new AegCombinedSplit();
                temp.addSplit(aegSplit);
                map.put(lastLocation, temp);

            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
        for (AegCombinedSplit split : map.values()) {
            combinedSplits.add(split);
        }

        log.info("Original split count: {}", splits.size());
        log.info("Combined split count: {}", combinedSplits.size());
        return combinedSplits;
    }
}
