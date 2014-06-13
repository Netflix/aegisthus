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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.netflix.aegisthus.input.AegCombinedSplit;

public class CombineSSTableReader extends AegisthusRecordReader {
	private static final Log LOG = LogFactory.getLog(CombineSSTableReader.class);
	private TaskAttemptContext ctx;
	private AegCombinedSplit splits;
	private int current;
	private long curPos;
	private SSTableRecordReader reader;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext ctx) throws IOException, InterruptedException {
		this.ctx = ctx;
		splits = (AegCombinedSplit) split;
		LOG.info(String.format("splits to process: %d", splits.getSplits().size()));
		current = 0;
		reader = new SSTableRecordReader();
		reader.initialize(splits.getSplits().get(0), this.ctx);
		start = 0;
		end = splits.getLength();
		curPos = splits.getSplits().get(0).getStart();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		boolean resp = reader.nextKeyValue();
		pos += reader.pos - curPos;
		curPos = reader.pos;
		if (!resp) {
			current++;
			if (current >= splits.getSplits().size()) {
				return false;
			} else {
				reader.initialize(splits.getSplits().get(current), this.ctx);
				curPos = splits.getSplits().get(current).getStart();
				resp = reader.nextKeyValue();
			}
		}
		if (resp == true) {
			key.set(reader.key);
			value.set(reader.value);
		}
		return resp;
	}
}
