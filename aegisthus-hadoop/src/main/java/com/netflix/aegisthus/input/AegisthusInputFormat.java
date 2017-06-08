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

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.netflix.Aegisthus;
import com.netflix.aegisthus.input.readers.SSTableRecordReader;
import com.netflix.aegisthus.input.splits.AegCompressedSplit;
import com.netflix.aegisthus.input.splits.AegSplit;
import com.netflix.aegisthus.io.sstable.IndexDatabaseScanner;
import com.netflix.aegisthus.io.writable.AegisthusKey;
import com.netflix.aegisthus.io.writable.AtomWritable;
import org.apache.hadoop.conf.Configuration;
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
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The AegisthusInputFormat class handles creating splits and record readers.
 */
public class AegisthusInputFormat extends FileInputFormat<AegisthusKey, AtomWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(AegisthusInputFormat.class);
    private static final Pattern PATH_DATETIME_MATCHER = Pattern.compile(".+/-?\\d+/(\\d{12})/.+");

    @Override
    public RecordReader<AegisthusKey, AtomWritable> createRecordReader(InputSplit inputSplit,
            TaskAttemptContext context) throws IOException, InterruptedException {
        return new SSTableRecordReader();
    }

    /**
     * Look for a CompressionInfo.db file that matches the given Data.db file.
     *
     * @param fs the path file system
     * @param dataDbPath the reference to a Data.db file
     * @return an optional CompressionInfo.db file that matches the Data.db file.
     * @throws IOException on file system errors.
     */
    Optional<Path> getCompressionPath(FileSystem fs, Path dataDbPath) throws IOException {
        final String fullPath = dataDbPath.toString().replaceAll("-Data.db", "-CompressionInfo.db");
        Path testPath = new Path(fullPath);
        if (fs.exists(testPath)) {
            return Optional.of(testPath);
        }

        // If a CompressionInfo file wasn't found in the same directory, check to see if the path has a date time in it
        Matcher matcher = PATH_DATETIME_MATCHER.matcher(fullPath);
        if (!matcher.matches()) {
            return Optional.absent();
        }

        // the path looks like it has a date time, we will check the adjacent minutes for the CompressionInfo file also.
        String dateTime = matcher.group(1);
        long dateTimeNumeric = Long.valueOf(dateTime);

        // check the next minute
        testPath = new Path(fullPath.replace("/" + dateTime + "/", "/" + Long.toString(dateTimeNumeric + 1) + "/"));
        if (fs.exists(testPath)) {
            return Optional.of(testPath);
        }

        // check the previous minute
        testPath = new Path(fullPath.replace("/" + dateTime + "/", "/" + Long.toString(dateTimeNumeric - 1) + "/"));
        if (fs.exists(testPath)) {
            return Optional.of(testPath);
        }

        return Optional.absent();
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
        Configuration conf = job.getConfiguration();
        FileSystem fs = path.getFileSystem(conf);
        BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);

        Optional<Path> compressionPath = getCompressionPath(fs, path);
        if (compressionPath.isPresent()) {
            return ImmutableList.of((InputSplit) AegCompressedSplit.createAegCompressedSplit(path, 0, length,
                    blkLocations[blkLocations.length - 1].getHosts(), compressionPath.get(), conf));
        }

        long blockSize = file.getBlockSize();
        String aegisthusBlockSize = conf.get(Aegisthus.Feature.CONF_BLOCKSIZE);
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
    public List<InputSplit> getSplits(final JobContext job) throws IOException {
        // TODO switch to Java 8 and replace this with streams
        List<FileStatus> allFiles = listStatus(job);
        List<FileStatus> dataFiles = Lists.newLinkedList();
        final Queue<InputSplit> allSplits = new ConcurrentLinkedQueue<>();

        for (FileStatus file : allFiles) {
            String name = file.getPath().getName();
            if (name.endsWith("-Data.db")) {
                dataFiles.add(file);
            }
        }

        LOG.info("Calculating splits for {} input files of which {} are data files", allFiles.size(), dataFiles.size());

        // TODO make the number of threads configurable
        ListeningExecutorService service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(20));
        for (final FileStatus file : dataFiles) {
            ListenableFuture<List<InputSplit>> future = service.submit(new Callable<List<InputSplit>>() {
                public List<InputSplit> call() throws IOException {
                    List<InputSplit> splitsForFile = getSSTableSplitsForFile(job, file);
                    LOG.info("Split '{}' into {} splits", file.getPath(), splitsForFile.size());

                    return splitsForFile;
                }
            });
            Futures.addCallback(future, new FutureCallback<List<InputSplit>>() {
                public void onFailure(Throwable thrown) {
                    throw Throwables.propagate(thrown);
                }

                public void onSuccess(List<InputSplit> splits) {
                    allSplits.addAll(splits);
                }
            });
        }

        try {
            service.shutdown();
            // TODO timeout configurable
            service.awaitTermination(2, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
        ImmutableList<InputSplit> inputSplits = ImmutableList.copyOf(allSplits);

        LOG.info("Split {} input data files into {} parts", dataFiles.size(), inputSplits.size());
        return inputSplits;
    }
}
