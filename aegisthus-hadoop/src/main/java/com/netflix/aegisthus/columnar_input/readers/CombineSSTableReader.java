package com.netflix.aegisthus.columnar_input.readers;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.netflix.aegisthus.columnar_input.splits.AegCombinedSplit;

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
		if (resp) {
			key = reader.key;
			value = reader.value;
		}
		return resp;
	}
}
