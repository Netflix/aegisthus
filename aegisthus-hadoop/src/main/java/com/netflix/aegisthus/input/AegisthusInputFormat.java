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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.netflix.Aegisthus;
import com.netflix.aegisthus.input.readers.SSTableRecordReader;
import com.netflix.aegisthus.input.splits.AegCompressedSplit;
import com.netflix.aegisthus.input.splits.AegSplit;
import com.netflix.aegisthus.io.sstable.IndexDatabaseScanner;
import com.netflix.aegisthus.io.writable.AegisthusKey;
import com.netflix.aegisthus.io.writable.AtomWritable;
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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * The AegisthusInputFormat class handles creating splits and record readers.
 */
public class AegisthusInputFormat extends FileInputFormat<AegisthusKey, AtomWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(AegisthusInputFormat.class);

    @Override
    public RecordReader<AegisthusKey, AtomWritable> createRecordReader(InputSplit inputSplit,
            TaskAttemptContext context) throws IOException, InterruptedException {
        return new SSTableRecordReader();
    }

    /**
     * The main thing that the addSSTableSplit handles is to split SSTables
     * using their index if available. The general algorithm is that if the file
     * is large than the blocksize plus some fuzzy factor to
     */
    List<InputSplit> getSSTableSplitsForFile(JobContext job, FileStatus file) throws IOException {
        long length = file.getLen();
        if (length == 0) {
            LOG.info("skipping zero length file: {}", file.getPath());
            return Collections.emptyList();
        }

        Path path = file.getPath();
        FileSystem fs = path.getFileSystem(job.getConfiguration());
        BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);

        Path compressionPath = new Path(path.getParent(), path.getName().replaceAll("-Data.db", "-CompressionInfo.db"));
        if (fs.exists(compressionPath)) {
            return ImmutableList.of((InputSplit) AegCompressedSplit.createAegCompressedSplit(path, 0, length,
                    blkLocations[blkLocations.length - 1].getHosts(), compressionPath));
        }

        long blockSize = file.getBlockSize();
        String aegisthusBlockSize = job.getConfiguration().get(Aegisthus.Feature.CONF_BLOCKSIZE);
        if (!Strings.isNullOrEmpty(aegisthusBlockSize)) {
            blockSize = Long.valueOf(aegisthusBlockSize);
        }
        long maxSplitSize = (long) (blockSize * .99);
        long fuzzySplit = (long) (blockSize * 1.2);

        long bytesRemaining = length;

        List<InputSplit> splits = Lists.newArrayList();
        IndexDatabaseScanner scanner = null;
        // Only initialize if we are going to have more than a single split
        if (fuzzySplit < length) {
            Path indexPath = new Path(path.getParent(), path.getName().replaceAll("-Data.db", "-Index.db"));
            if (!fs.exists(indexPath)) {
                fuzzySplit = length;
            } else {
                FSDataInputStream fileIn = fs.open(indexPath);
                scanner = new IndexDatabaseScanner(new BufferedInputStream(fileIn));
            }
        }

        long splitStart = 0;
        while (splitStart + fuzzySplit < length && scanner != null && scanner.hasNext()) {
            long splitSize = 0;
            // The scanner returns an offset from the start of the file.
            while (splitSize < maxSplitSize && scanner.hasNext()) {
                IndexDatabaseScanner.OffsetInfo offsetInfo = scanner.next();
                splitSize = offsetInfo.getDataFileOffset() - splitStart;

            }
            int blkIndex = getBlockIndex(blkLocations, splitStart + (splitSize / 2));
            LOG.debug("split path: {}:{}:{}", path.getName(), splitStart, splitSize);
            splits.add(AegSplit.createSplit(path, splitStart, splitSize, blkLocations[blkIndex].getHosts()));
            bytesRemaining -= splitSize;
            splitStart += splitSize;
        }

        if (scanner != null) {
            scanner.close();
        }

        if (bytesRemaining != 0) {
            LOG.debug("end path: {}:{}:{}", path.getName(), length - bytesRemaining, bytesRemaining);
            splits.add(AegSplit.createSplit(path, length - bytesRemaining, bytesRemaining,
                    blkLocations[blkLocations.length - 1].getHosts()));
        }

        return splits;
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<FileStatus> files = listStatus(job);

        List<InputSplit> splits = Lists.newArrayListWithExpectedSize(files.size());
        for (FileStatus file : files) {
            String name = file.getPath().getName();
            if (name.endsWith("-Data.db")) {
                splits.addAll(getSSTableSplitsForFile(job, file));
            }
        }
        return splits;
    }
}
