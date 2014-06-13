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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class AegCombinedSplit extends InputSplit implements Writable {
	List<AegSplit> splits = Lists.newArrayList();

	public AegCombinedSplit() {
	};

	public AegCombinedSplit(AegSplit split) {
		splits.add(split);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int cnt = in.readInt();
		for (int i = 0; i < cnt; i++) {
			AegSplit split = new AegSplit();
			split.readFields(in);
			splits.add(split);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(splits.size());
		for (AegSplit split : splits) {
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

	public List<AegSplit> getSplits() {
		return splits;
	}

}
