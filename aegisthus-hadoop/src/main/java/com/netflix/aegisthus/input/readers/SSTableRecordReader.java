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
package com.netflix.aegisthus.input.readers;

import java.io.DataInputStream;
import java.io.IOError;
import java.io.IOException;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.netflix.aegisthus.input.AegSplit;
import com.netflix.aegisthus.io.sstable.SSTableScanner;

public class SSTableRecordReader extends AegisthusRecordReader {
	private static final Log LOG = LogFactory.getLog(SSTableRecordReader.class);
	private SSTableScanner scanner;
	private boolean outputFile = false;
	private String filename = null;

	@Override
	public void close() throws IOException {
		super.close();
		if (scanner != null) {
			scanner.close();
		}
	}

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext ctx) throws IOException, InterruptedException {
		AegSplit split = (AegSplit) inputSplit;

		start = split.getStart();
		end = split.getEnd();
		outputFile = ctx.getConfiguration().getBoolean("aegsithus.debug.file", false);
		filename = split.getPath().toUri().toString();

		LOG.info(String.format("File: %s", split.getPath().toUri().getPath()));
		LOG.info("Start: " + start);
		LOG.info("End: " + end);

		try {
			scanner = new SSTableScanner(	new DataInputStream(split.getInput(ctx.getConfiguration())),
											split.getConvertors(),
											end,
											Descriptor.fromFilename(filename).version);
			scanner.skipUnsafe(start);
			this.pos = start;
		} catch (IOException e) {
			throw new IOError(e);
		}

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (pos >= end) {
			return false;
		}
		String json = null;
		json = scanner.next();
		pos += scanner.getDatasize();
		json = json.trim();
		if (outputFile) {
			key.set(filename);
		} else {
			// a quick hack to pull out the rowkey from the json.
			key.set(json.substring(2, json.indexOf(':') - 1));
		}
		value.set(json);
		return true;
	}
}