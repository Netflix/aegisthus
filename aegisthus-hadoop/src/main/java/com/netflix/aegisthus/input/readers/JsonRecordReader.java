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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.netflix.aegisthus.input.AegSplit;

public class JsonRecordReader extends AegisthusRecordReader {
	protected BufferedReader reader;

	@Override
	public void close() throws IOException {
		if (reader != null) {
			reader.close();
		}
	}

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext ctx) throws IOException, InterruptedException {
		AegSplit split = (AegSplit) inputSplit;
		InputStream is = split.getInput(ctx.getConfiguration());
		start = split.getStart();
		end = split.getEnd();
		pos = start;
		is.skip(split.getStart());
		if (split.getPath().getName().endsWith(".gz")) {
			reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(is)));
		} else {
			reader = new BufferedReader(new InputStreamReader(is));
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		String line = reader.readLine();
		if (line == null) {
			return false;
		}
		pos += line.length();
		String[] text = line.split("\t", 2);
		key.set(text[0]);
		value.set(text[1]);
		return true;
	}

}
