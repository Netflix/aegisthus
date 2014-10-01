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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.aegisthus.columnar_input.AegisthusInputFormat;
import com.netflix.aegisthus.input.AegisthusCombinedInputFormat;
import com.netflix.aegisthus.io.writable.AtomWritable;
import com.netflix.aegisthus.io.writable.CompositeKey;
import com.netflix.aegisthus.mapred.reduce.CassReducer;
import com.netflix.aegisthus.mapred.reduce.CassSSTableReducer;
import com.netflix.aegisthus.output.AegisthusOutputFormat;
import com.netflix.aegisthus.tools.DirectoryWalker;
import com.netflix.aegisthus.tools.StorageHelper;
import com.netflix.hadoop.output.CleanOutputFormat;

public class Aegisthus extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(Aegisthus.class);
    public static class ColumnarMap extends Mapper<CompositeKey, AtomWritable, CompositeKey, AtomWritable> {
        @Override
        protected void map(CompositeKey key, AtomWritable value, Context context) throws IOException,
                InterruptedException {
            context.write(key, value);
        }
    }

    public static class TextMap extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
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
    private static final String OPT_OUTPUT = "output";
    private static final String OPT_PRODUCESSTABLE = "produceSSTable";

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Aegisthus(), args);

        boolean exit = Boolean.valueOf(System.getProperty("aegisthus.exit", "true"));
        if (exit) {
            System.exit(res);
        } else if(res != 0) {
            throw new RuntimeException("Unexpected exit code");
        }
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
        opts.addOption(OptionBuilder.withArgName(OPT_PRODUCESSTABLE)
                .withDescription("produces sstable output (default is to produce json)")
                .create(OPT_PRODUCESSTABLE));
        CommandLineParser parser = new GnuParser();

        try {
            CommandLine cl = parser.parse(opts, args, true);
            if (!(cl.hasOption(OPT_INPUT) || cl.hasOption(OPT_INPUTDIR))) {
                System.out.println("Must have either an input or inputDir option");
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp(String.format("hadoop jar aegisthus.jar %s", Aegisthus.class.getName()), opts);
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
        Job job = Job.getInstance(getConf());

        job.setJarByClass(Aegisthus.class);
        CommandLine cl = getOptions(args);
        if (cl == null) {
            return 1;
        }

        return cl.hasOption(OPT_PRODUCESSTABLE) ? runColumnar(job, cl) : runJson(job, cl);
    }

    public int runColumnar(Job job, CommandLine cl) throws Exception {
        job.setInputFormatClass(AegisthusInputFormat.class);
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(AtomWritable.class);
        job.setOutputFormatClass(CleanOutputFormat.class);
        job.setMapperClass(ColumnarMap.class);
        job.setReducerClass(CassSSTableReducer.class);
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
        TextInputFormat.setInputPaths(job, paths.toArray(new Path[paths.size()]));
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

    public int runJson(Job job, CommandLine cl) throws Exception {
        job.getConfiguration().set("aeg.temp.dir", "/tmp/" + UUID.randomUUID());
        Path temp = new Path(job.getConfiguration().get("aeg.temp.dir"));
        FileSystem fs = temp.getFileSystem(job.getConfiguration());
        fs.mkdirs(temp);

        job.setInputFormatClass(AegisthusCombinedInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(AegisthusOutputFormat.class);
        job.setMapperClass(TextMap.class);
        job.setReducerClass(CassReducer.class);
        List<Path> paths = Lists.newArrayList();
        if (cl.hasOption(OPT_INPUT)) {
            for (String input : cl.getOptionValues(OPT_INPUT)) {
                job.getConfiguration().set("aegisthus.json.dir", input.replace("/*.gz", ""));
                paths.add(new Path(input));
            }
        }
        if (cl.hasOption(OPT_INPUTDIR)) {
            paths.addAll(getDataFiles(job.getConfiguration(), cl.getOptionValue(OPT_INPUTDIR)));
        }
        TextInputFormat.setInputPaths(job, paths.toArray(new Path[paths.size()]));
        AegisthusOutputFormat.setOutputPath(job, new Path(cl.getOptionValue(OPT_OUTPUT)));

        job.submit();
        System.out.println(job.getJobID());
        System.out.println(job.getTrackingURL());

        boolean success = job.waitForCompletion(true);
        fs.delete(temp, true);
        return success ? 0 : 1;
    }
}
