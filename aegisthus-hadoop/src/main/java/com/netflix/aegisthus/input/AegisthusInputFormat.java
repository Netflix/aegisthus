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
package com.netflix.aegisthus.input;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.aegisthus.input.AegSplit.Type;
import com.netflix.aegisthus.input.readers.CombineSSTableReader;
import com.netflix.aegisthus.input.readers.CommitLogRecordReader;
import com.netflix.aegisthus.input.readers.JsonRecordReader;
import com.netflix.aegisthus.input.readers.SSTableRecordReader;
import com.netflix.aegisthus.io.sstable.OffsetScanner;
import com.netflix.aegisthus.io.sstable.SSTableScanner;

/**
 * The AegisthusInputFormat class handles creating splits and reading sstables,
 * commitlogs and json.
 */
public class AegisthusInputFormat extends FileInputFormat<Text, Text> {
	private static final Log LOG = LogFactory.getLog(AegisthusInputFormat.class);
	public static final String COLUMN_TYPE = "aegisthus.columntype";
	public static final String KEY_TYPE = "aegisthus.keytype";
	@SuppressWarnings("rawtypes")
	protected Map<String, AbstractType> convertors;
	private List<FileStatus> overrideListStatus = null;

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) {
		AegSplit split = null;
		if (inputSplit instanceof AegCombinedSplit) {
			return new CombineSSTableReader();
		}
		split = (AegSplit) inputSplit;
		RecordReader<Text, Text> reader = null;
		switch (split.getType()) {
		case json:
			reader = new JsonRecordReader();
			break;
		case sstable:
			reader = new SSTableRecordReader();
			break;
		case commitlog:
			reader = new CommitLogRecordReader();
			break;
		}
		return reader;
	}

	@SuppressWarnings("rawtypes")
	private Map<String, AbstractType> initConvertors(JobContext job) throws IOException {
		Map<String, AbstractType> convertors = Maps.newHashMap();
		String conversion = job.getConfiguration().get(KEY_TYPE);
		LOG.info(KEY_TYPE + ": " + conversion);
		if (conversion != null) {
			try {
				convertors.put(SSTableScanner.KEY, TypeParser.parse(conversion));
			} catch (ConfigurationException e) {
				throw new IOException(e);
			} catch (SyntaxException e) {
				throw new IOException(e);
            }
		}
		conversion = job.getConfiguration().get(COLUMN_TYPE);
		LOG.info(COLUMN_TYPE + ": " + conversion);
		if (conversion != null) {
			try {
				convertors.put(SSTableScanner.COLUMN_NAME_KEY, TypeParser.parse(conversion));
			} catch (ConfigurationException e) {
				throw new IOException(e);
			} catch (SyntaxException e) {
				throw new IOException(e);
            }
		}

		if (convertors.size() == 0) {
			return null;
		}
		return convertors;
	}

	/**
	 * The main thing that the addSSTableSplit handles is to split SSTables
	 * using their index if available. The general algorithm is that if the file
	 * is large than the blocksize plus some fuzzy factor to
	 */
	public void addSSTableSplit(List<InputSplit> splits, JobContext job, FileStatus file) throws IOException {
		Path path = file.getPath();
		FileSystem fs = path.getFileSystem(job.getConfiguration());
		long length = file.getLen();
		BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
		if (length != 0) {
			long blockSize = file.getBlockSize();
			long maxSplitSize = (long) (blockSize * .99);
			long fuzzySplit = (long) (blockSize * 1.2);

			long bytesRemaining = length;

			Path compressionPath = new Path(path.getParent(), path.getName().replaceAll("-Data.db",
																						"-CompressionInfo.db"));
			if (!fs.exists(compressionPath)) {
				OffsetScanner scanner = null;
				// Only initialize if we are going to have more than a single
				// split
				if (fuzzySplit < length) {
					Path indexPath = new Path(path.getParent(), path.getName().replaceAll("-Data.db", "-Index.db"));
					if (!fs.exists(indexPath)) {
						fuzzySplit = length;
					} else {
						FSDataInputStream fileIn = fs.open(indexPath);
						scanner = new OffsetScanner(new DataInputStream(new BufferedInputStream(fileIn)), indexPath.getName());
					}
				}
				long splitStart = 0;
				while (splitStart + fuzzySplit < length && scanner.hasNext()) {
					long splitSize = 0;
					// The scanner returns an offset from the start of the file.
					while (splitSize < maxSplitSize && scanner.hasNext()) {
                        Pair<Long, Long> pair = scanner.next();
                        splitSize = pair.left - splitStart;
					}
					int blkIndex = getBlockIndex(blkLocations, splitStart + (splitSize / 2));
					LOG.info("split path: " + path.getName() + ":" + splitStart + ":" + splitSize);
					splits
							.add(new AegSplit(	path,
												splitStart,
												splitSize,
												blkLocations[blkIndex].getHosts(),
												convertors));
					bytesRemaining -= splitSize;
					splitStart += splitSize;
				}
				if (scanner != null ) {
					scanner.close();
				}
			}

			if (bytesRemaining != 0) {
				LOG.info("end path: " + path.getName() + ":" + (length - bytesRemaining) + ":" + bytesRemaining);
				splits.add(new AegSplit(path,
										length - bytesRemaining,
										bytesRemaining,
										blkLocations[blkLocations.length - 1].getHosts(),
										convertors,
										fs.exists(compressionPath),
										compressionPath));
			}
		} else {
			LOG.info("skipping zero length file: " + path.toString());
		}
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		List<InputSplit> splits = Lists.newArrayList();
		List<FileStatus> files = listStatus(job);
		convertors = initConvertors(job);
		for (FileStatus file : files) {
			String name = file.getPath().getName();
			if (name.endsWith("-Data.db")) {
				addSSTableSplit(splits, job, file);
			} else if (name.startsWith("CommitLog")) {
				LOG.info(String.format("adding %s as a CommitLog split", file.getPath().toUri().toString()));
				BlockLocation[] blkLocations = file
						.getPath()
						.getFileSystem(job.getConfiguration())
						.getFileBlockLocations(file, 0, file.getLen());
				splits.add(new AegSplit(file.getPath(),
										0,
										file.getLen(),
										blkLocations[0].getHosts(),
										Type.commitlog,
										convertors));
			} else {
				LOG.info(String.format("adding %s as a json split", file.getPath().toUri().toString()));
				BlockLocation[] blkLocations = file
						.getPath()
						.getFileSystem(job.getConfiguration())
						.getFileBlockLocations(file, 0, file.getLen());
				splits.add(new AegSplit(file.getPath(), 0, file.getLen(), blkLocations[0].getHosts(), Type.json));
			}
		}
		return splits;
	}

	/**
	 * If we have been given a list of statuses to operate on, use those instead of falling back to the base method.
	 */
	@Override
	protected List<FileStatus> listStatus(JobContext job) throws IOException {
		if (overrideListStatus == null) {
			return super.listStatus(job);
		} else {
			return overrideListStatus;
		}
	}

	/**
	 * Instead of using the base listStatus, return this list of file statuses
	 *
	 * @param overrideListStatus the list of statuses to be returned for all future calls to this.listStatus()
	 */
	public void setOverrideListStatus(List<FileStatus> overrideListStatus) {
		this.overrideListStatus = overrideListStatus;
	}
}
