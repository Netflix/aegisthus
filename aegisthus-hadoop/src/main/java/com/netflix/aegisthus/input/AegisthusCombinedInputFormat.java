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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * This class takes the splits created by the AegisthusInputFormat and combines
 * small SSTables into single splits.
 */
public class AegisthusCombinedInputFormat extends AegisthusInputFormat {
	private static final Log LOG = LogFactory.getLog(AegisthusCombinedInputFormat.class);
	private int maxSplitSize = 104857600;

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		List<InputSplit> splits = super.getSplits(job);
		List<InputSplit> combinedSplits = Lists.newArrayList();
		Map<String, AegCombinedSplit> map = Maps.newHashMap();
		int cnt = 0;
		int cntAdded = 0;
		
		top: for (InputSplit split : splits) {
			AegSplit aegSplit = (AegSplit) split;
			switch (aegSplit.type) {
			case sstable:
				cnt++;
				if (aegSplit.getLength() >= maxSplitSize) {
					cntAdded++;
					combinedSplits.add(aegSplit);
					continue;
				}
				try {
					String lastLocation = null;
					for (String location : aegSplit.getLocations()) {
						lastLocation = location;
						if (map.containsKey(location)) {
							AegCombinedSplit temp = map.get(location);
							cntAdded++;
							temp.getSplits().add(aegSplit);
							if (temp.getLength() >= maxSplitSize) {
								combinedSplits.add(temp);
								map.remove(location);
							}
							continue top;
						}
					}
					cntAdded++;
					AegCombinedSplit temp = new AegCombinedSplit(aegSplit);
					map.put(lastLocation, temp);

				} catch (InterruptedException e) {
					throw new IOException(e);
				}

				break;
			default:
				combinedSplits.add(aegSplit);
			}
		}
		for(AegCombinedSplit split: map.values()) {
				combinedSplits.add(split);
		}

		LOG.info(String.format("sstable AegSplits: %d", cnt));
		LOG.info(String.format("sstables Added AegSplits: %d", cntAdded));
		LOG.info(String.format("other AegSplits: %d", splits.size() - cnt));
		LOG.info(String.format("AegCombinedSplits: %d", combinedSplits.size()));
		return combinedSplits;
	}

}
