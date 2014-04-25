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
import java.io.InputStream;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.netflix.aegisthus.input.AegSplit;
import com.netflix.aegisthus.io.sstable.SSTableScanner;

public class SSTableRecordReader extends AegisthusRecordReader {
	private static final Log LOG = LogFactory.getLog(SSTableRecordReader.class);
	private SSTableScanner scanner;
	private boolean outputFile = false;
	private String filename = null;
	private long errorRows = 0;
	@SuppressWarnings("rawtypes")
	private TaskInputOutputContext context = null;

	@Override
	public void close() throws IOException {
		super.close();
		if (scanner != null) {
			scanner.close();
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext ctx) throws IOException, InterruptedException {
		AegSplit split = (AegSplit) inputSplit;

		start = split.getStart();
		//TODO: This has a side effect of setting compressionmetadata. remove this.
		InputStream is = split.getInput(ctx.getConfiguration());
		if (split.isCompressed()) {
			end = split.getCompressionMetadata().getDataLength();
		} else {
			end = split.getEnd();
		}
		outputFile = ctx.getConfiguration().getBoolean("aegsithus.debug.file", false);
		filename = split.getPath().toUri().toString();

		LOG.info(String.format("File: %s", split.getPath().toUri().getPath()));
		LOG.info("Start: " + start);
		LOG.info("End: " + end);
		if (ctx instanceof TaskInputOutputContext) {
			context = (TaskInputOutputContext) ctx;
		}

		try {
			scanner = new SSTableScanner(new DataInputStream(is),
					split.getConvertors(), end, Descriptor.fromFilename(filename).version);
			if (ctx.getConfiguration().get("aegisthus.maxcolsize") != null) {
				scanner.setMaxColSize(ctx.getConfiguration().getLong("aegisthus.maxcolsize", -1L));
				LOG.info(String.format("aegisthus.maxcolsize - %d",
						ctx.getConfiguration().getLong("aegisthus.maxcolsize", -1L)));
			}
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
		if (context != null && scanner.getErrorRowCount() > errorRows) {
			errorRows++;
			context.getCounter("aegisthus", "rowsTooBig").increment(1L);
		}
		value.set(json);
		return true;
	}
}