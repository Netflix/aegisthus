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
package com.netflix.aegisthus.columnar_input;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.List;

import org.apache.cassandra.utils.Pair;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.netflix.aegisthus.columnar_input.readers.CombineSSTableReader;
import com.netflix.aegisthus.columnar_input.readers.CommitLogRecordReader;
import com.netflix.aegisthus.columnar_input.readers.SSTableRecordReader;
import com.netflix.aegisthus.columnar_input.splits.AegCombinedSplit;
import com.netflix.aegisthus.columnar_input.splits.AegCompressedSplit;
import com.netflix.aegisthus.columnar_input.splits.AegSplit;
import com.netflix.aegisthus.columnar_input.splits.AegSplit.Type;
import com.netflix.aegisthus.io.sstable.OffsetScanner;
import com.netflix.aegisthus.io.writable.AtomWritable;
import com.netflix.aegisthus.io.writable.CompositeKey;

/**
 * The AegisthusInputFormat class handles creating splits and reading sstables,
 * commitlogs and json.
 */
public class AegisthusInputFormat extends FileInputFormat<CompositeKey, AtomWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(AegisthusInputFormat.class);

    @Override
    public RecordReader<CompositeKey, AtomWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) {
        AegSplit split = null;
        if (inputSplit instanceof AegCombinedSplit) {
            return new CombineSSTableReader();
        }
        split = (AegSplit) inputSplit;
        RecordReader<CompositeKey, AtomWritable> reader = null;
        switch (split.getType()) {
        case sstable:
            reader = new SSTableRecordReader();
            break;
        case commitlog:
            reader = new CommitLogRecordReader();
            break;
        }
        return reader;
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
                Path indexPath = null;
                if (fuzzySplit < length) {
                    indexPath = new Path(path.getParent(), path.getName().replaceAll("-Data.db", "-Index.db"));
                    if (!fs.exists(indexPath)) {
                        fuzzySplit = length;
                    } else {
                        FSDataInputStream fileIn = fs.open(indexPath);
                        scanner = new OffsetScanner(new BufferedInputStream(fileIn), indexPath.getName());
                    }
                }
                long splitStart = 0;
                long indexOffset = 0;
                long newIndexOffset = 0;
                while (splitStart + fuzzySplit < length && scanner != null && scanner.hasNext()) {
                    long splitSize = 0;
                    // The scanner returns an offset from the start of the file.
                    while (splitSize < maxSplitSize && scanner.hasNext()) {
                        Pair<Long, Long> pair = scanner.next();
                        splitSize = pair.left - splitStart;
                        newIndexOffset = pair.right;

                    }
                    int blkIndex = getBlockIndex(blkLocations, splitStart + (splitSize / 2));
                    LOG.debug("split path: {}:{}:{}", path.getName(), splitStart, splitSize);
                    splits.add(new AegSplit(path, splitStart, splitSize, blkLocations[blkIndex].getHosts()));
                    indexOffset = newIndexOffset;
                    bytesRemaining -= splitSize;
                    splitStart += splitSize;
                }
                if (scanner != null) {
                    scanner.close();
                }
            }

            if (bytesRemaining != 0) {
                LOG.debug("end path: {}:{}:{}", path.getName(), length - bytesRemaining, bytesRemaining);
                if (fs.exists(compressionPath)) {
                    splits.add(new AegCompressedSplit(path, length - bytesRemaining, bytesRemaining,
                            blkLocations[blkLocations.length - 1].getHosts(), compressionPath));
                } else {
                    splits.add(new AegSplit(path, length - bytesRemaining, bytesRemaining,
                            blkLocations[blkLocations.length - 1].getHosts()));
                }
            }
        } else {
            LOG.info("skipping zero length file: {}", path.toString());
        }
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> splits = Lists.newArrayList();
        List<FileStatus> files = listStatus(job);
        for (FileStatus file : files) {
            String name = file.getPath().getName();
            if (name.endsWith("-Data.db")) {
                addSSTableSplit(splits, job, file);
            } else if (name.startsWith("CommitLog")) {
                LOG.info(String.format("adding %s as a CommitLog split", file.getPath().toUri().toString()));
                BlockLocation[] blkLocations = file.getPath()
                        .getFileSystem(job.getConfiguration())
                        .getFileBlockLocations(file, 0, file.getLen());
                splits.add(new AegSplit(file.getPath(), 0, file.getLen(), blkLocations[0].getHosts(), Type.commitlog));
            }
        }
        return splits;
    }
}
