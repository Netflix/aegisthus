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
package com.netflix;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.aegisthus.io.writable.AtomWritable;
import com.netflix.aegisthus.io.writable.CompositeKey;
import com.netflix.aegisthus.tools.DirectoryWalker;
import com.netflix.aegisthus.tools.StorageHelper;
import com.netflix.hadoop.output.CleanOutputFormat;

public class Aegisthus extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(Aegisthus.class);
    public static class Map extends Mapper<CompositeKey, AtomWritable, CompositeKey, AtomWritable> {
        @Override
        protected void map(CompositeKey key, AtomWritable value, Context context) throws IOException,
                InterruptedException {
            context.write(key, value);
        }
    }

    public static class Partition extends Partitioner<CompositeKey, AtomWritable> {
        @Override
        public int getPartition(CompositeKey key, AtomWritable value, int numPartitions) {
            return Math.abs(key.getKey().hashCode() % numPartitions);
        }

    }

    public static class RowKeyGroupingComparator extends WritableComparator {
        public RowKeyGroupingComparator() {
            super(CompositeKey.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable wc1, WritableComparable wc2) {
            CompositeKey ck1 = (CompositeKey) wc1;
            CompositeKey ck2 = (CompositeKey) wc2;
            return ck1.getKey().compareTo(ck2.getKey());
        }
    }

    public static class CompositeKeyComparator extends WritableComparator implements Configurable {
        private Comparator<ByteBuffer> comparator;
        private Configuration conf;

        protected CompositeKeyComparator() {
            super(CompositeKey.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable wc1, WritableComparable wc2) {
            CompositeKey ck1 = (CompositeKey) wc1;
            CompositeKey ck2 = (CompositeKey) wc2;
            ck1.setComparator(comparator);
            return ck1.compareTo(ck2);
        }

        @Override
        public Configuration getConf() {
            return conf;
        }

        @Override
        public void setConf(Configuration conf) {
            this.conf = conf;
            String comparatorType = conf.get("aegisthus.columntype");
            try {
                comparator = TypeParser.parse(comparatorType);
            } catch (SyntaxException e) {
                throw new RuntimeException(e);
            } catch (ConfigurationException e) {
                throw new RuntimeException(e);
            }

        }
    }

    private static final String OPT_INPUT = "input";
    private static final String OPT_INPUTDIR = "inputDir";
    private static final String OPT_INPUTFORMATCLASSNAME = "inputFormatClassName";
    private static final String OPT_OUTPUT = "output";
    private static final String OPT_REDUCERCLASSNAME = "reducerClassName";

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Aegisthus(), args);

        System.exit(res);
    }

    protected List<Path> getDataFiles(Configuration conf, String dir) throws IOException {
        Set<String> globs = Sets.newHashSet();
        List<Path> output = Lists.newArrayList();
        Path dirPath = new Path(dir);
        FileSystem fs = dirPath.getFileSystem(conf);
        List<FileStatus> input = Lists.newArrayList(fs.listStatus(dirPath));
        for (String path : DirectoryWalker.with(conf).threaded().addAllStatuses(input).pathsString()) {
            if (path.endsWith("-Data.db")) {
                globs.add(path.replaceAll("[^/]+-Data.db", "*-Data.db"));
            }
        }
        for (String path : globs) {
            output.add(new Path(path));
        }
        return output;
    }

    @SuppressWarnings("static-access")
    public CommandLine getOptions(String[] args) {
        Options opts = new Options();
        opts.addOption(OptionBuilder.withArgName(OPT_INPUT)
                .withDescription("Each input location")
                .hasArgs()
                .create(OPT_INPUT));
        opts.addOption(OptionBuilder.withArgName(OPT_OUTPUT)
                .isRequired()
                .withDescription("output location")
                .hasArg()
                .create(OPT_OUTPUT));
        opts.addOption(OptionBuilder.withArgName(OPT_INPUTDIR)
                .withDescription("a directory from which we will recursively pull sstables")
                .hasArgs()
                .create(OPT_INPUTDIR));
        opts.addOption(OptionBuilder.withArgName(OPT_INPUTFORMATCLASSNAME)
                .withDescription("full class name to use for the input format class")
                .hasArg()
                .create(OPT_INPUTFORMATCLASSNAME));
        opts.addOption(OptionBuilder.withArgName(OPT_REDUCERCLASSNAME)
                .withDescription("full class name to use for the reducer class")
                .hasArg()
                .create(OPT_REDUCERCLASSNAME));
        CommandLineParser parser = new GnuParser();

        try {
            CommandLine cl = parser.parse(opts, args, true);
            if (!(cl.hasOption(OPT_INPUT) || cl.hasOption(OPT_INPUTDIR))) {
                System.out.println("Must have either an input or inputDir option");
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp(String.format("hadoop jar aegsithus.jar %s", Aegisthus.class.getName()), opts);
                return null;
            }
            return cl;
        } catch (ParseException e) {
            System.out.println("Unexpected exception:" + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(String.format("hadoop jar aegisthus.jar %s", Aegisthus.class.getName()), opts);
            return null;
        }

    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());

        job.setJarByClass(Aegisthus.class);
        CommandLine cl = getOptions(args);
        if (cl == null) {
            return 1;
        }

        job.setInputFormatClass((Class<InputFormat>) Class.forName(
                cl.getOptionValue(OPT_INPUTFORMATCLASSNAME, "com.netflix.aegisthus.input.AegisthusInputFormat")
        ));
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(AtomWritable.class);
        job.setOutputFormatClass(CleanOutputFormat.class);
        job.setMapperClass(Map.class);
        job.setReducerClass((Class<Reducer>) Class.forName(
                cl.getOptionValue(OPT_REDUCERCLASSNAME, "com.netflix.aegisthus.mapred.reduce.CassReducer")
        ));
        job.setGroupingComparatorClass(RowKeyGroupingComparator.class);
        job.setPartitionerClass(Partition.class);
        job.setSortComparatorClass(CompositeKeyComparator.class);

        List<Path> paths = Lists.newArrayList();
        if (cl.hasOption(OPT_INPUT)) {
            for (String input : cl.getOptionValues(OPT_INPUT)) {
                paths.add(new Path(input));
            }
        }
        if (cl.hasOption(OPT_INPUTDIR)) {
            paths.addAll(getDataFiles(job.getConfiguration(), cl.getOptionValue(OPT_INPUTDIR)));
        }
        TextInputFormat.setInputPaths(job, paths.toArray(new Path[0]));
        Path temp = new Path("/tmp/" + UUID.randomUUID());
        TextOutputFormat.setOutputPath(job, temp);

        StorageHelper sh = new StorageHelper(job.getConfiguration());
        sh.setFinalPath(cl.getOptionValue(OPT_OUTPUT));
        LOG.info("temp location for job: {}", sh.getBaseTempLocation());

        job.submit();
        System.out.println(job.getJobID());
        System.out.println(job.getTrackingURL());
        boolean success = job.waitForCompletion(true);
        FileSystem fs = temp.getFileSystem(job.getConfiguration());
        if (fs.exists(temp)) {
            fs.delete(temp, true);
        }
        return success ? 0 : 1;
    }
}
