package com.netflix.aegisthus.columnar_input.splits;

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
	private final List<AegSplit> splits = Lists.newArrayList();

	public AegCombinedSplit() {
	}

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
		return locations.toArray(new String[locations.size()]);
	}

	public List<AegSplit> getSplits() {
		return splits;
	}

}
