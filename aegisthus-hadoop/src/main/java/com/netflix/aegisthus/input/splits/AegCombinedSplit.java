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
package com.netflix.aegisthus.input.splits;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public class AegCombinedSplit extends InputSplit implements Writable {
    private static final LoadingCache<String, Class<AegSplit>> AEG_SPLIT_LOADING_CACHE = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, Class<AegSplit>>() {
                @Override
                public Class<AegSplit> load(@Nonnull String className) throws Exception {
                    return (Class<AegSplit>) Class.forName(className);
                }
            });
    List<AegSplit> splits = Lists.newArrayList();

    public AegCombinedSplit() {
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        try {
            int cnt = in.readInt();
            for (int i = 0; i < cnt; i++) {
                String className = WritableUtils.readString(in);
                AegSplit split = AEG_SPLIT_LOADING_CACHE.get(className).newInstance();
                split.readFields(in);
                splits.add(split);
            }
        } catch (Throwable t) {
            Throwables.propagateIfPossible(t, IOException.class);
            throw new IOException("Unexpected exception", t);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(splits.size());
        for (AegSplit split : splits) {
            WritableUtils.writeString(out, split.getClass().getCanonicalName());
            split.write(out);
        }
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        int length = 0;
        for (AegSplit split : splits) {
            length += split.getLength();
        }
        return length;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        Set<String> locations = null;
        for (AegSplit split : splits) {
            Set<String> tempLocations = Sets.newHashSet(split.hosts);
            if (locations == null) {
                locations = tempLocations;
            } else {
                locations = Sets.intersection(locations, tempLocations);
            }
        }
        if (locations == null) {
            return null;
        }
        return locations.toArray(new String[0]);
    }

    public void addSplit(AegSplit split) {
        splits.add(split);
    }

    public List<AegSplit> getSplits() {
        return splits;
    }
}
