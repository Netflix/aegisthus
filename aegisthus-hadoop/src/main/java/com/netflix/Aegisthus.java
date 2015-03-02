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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.aegisthus.input.AegisthusInputFormat;
import com.netflix.aegisthus.io.writable.AegisthusKey;
import com.netflix.aegisthus.io.writable.AegisthusKeyGroupingComparator;
import com.netflix.aegisthus.io.writable.AegisthusKeyMapper;
import com.netflix.aegisthus.io.writable.AegisthusKeyPartitioner;
import com.netflix.aegisthus.io.writable.AegisthusKeySortingComparator;
import com.netflix.aegisthus.io.writable.AtomWritable;
import com.netflix.aegisthus.io.writable.RowWritable;
import com.netflix.aegisthus.mapreduce.CassSSTableReducer;
import com.netflix.aegisthus.output.CustomFileNameFileOutputFormat;
import com.netflix.aegisthus.output.JsonOutputFormat;
import com.netflix.aegisthus.output.SSTableOutputFormat;
import com.netflix.aegisthus.tools.DirectoryWalker;
import com.netflix.aegisthus.util.CFMetadataUtility;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

public class Aegisthus extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(Aegisthus.class);

    private static void logAegisthusVersion() {
        String classPath = Aegisthus.class.getResource("Aegisthus.class").toString();
        String manifestPath = classPath.replace("com/netflix/Aegisthus.class", "META-INF/MANIFEST.MF");
        try (InputStream inputStream = new URL(manifestPath).openStream()) {
            Manifest manifest = new Manifest(inputStream);
            Attributes attr = manifest.getMainAttributes();
            System.out.println("Running Aegisthus version " +
                            attr.getValue("Implementation-Version") +
                            " built from change " +
                            attr.getValue("Change") +
                            " on host " +
                            attr.getValue("Build-Host") +
                            " on " +
                            attr.getValue("Build-Date") +
                            " with Java " +
                            attr.getValue("Build-Java-Version")
            );
        } catch (IOException ignored) {
            System.out.println("Unable to locate Aegisthus manifest file");
        }
    }

    public static void main(String[] args) throws Exception {
        logAegisthusVersion();
        int res = ToolRunner.run(new Configuration(), new Aegisthus(), args);

        boolean exit = Boolean.valueOf(System.getProperty(Feature.CONF_SYSTEM_EXIT, "true"));
        if (exit) {
            System.exit(res);
        } else if (res != 0) {
            throw new RuntimeException("aegisthus finished with a non-zero exit code: " + res);
        }
    }

    private void setConfigurationFromCql(Configuration conf) {
        CFMetaData cfMetaData = CFMetadataUtility.initializeCfMetaData(conf);
        String keyType = cfMetaData.getKeyValidator().toString();
        String columnType = cfMetaData.comparator.toString();

        LOG.info("From CQL3, setting keyType({}) and columnType({}).", keyType, columnType);

        conf.set(Feature.CONF_KEYTYPE, keyType);
        conf.set(Feature.CONF_COLUMNTYPE, columnType);
    }

    List<Path> getDataFiles(Configuration conf, String dir) throws IOException {
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
    CommandLine getOptions(String[] args) {
        Options opts = new Options();
        opts.addOption(OptionBuilder.withArgName(Feature.CMD_ARG_INPUT_FILE)
                .withDescription("Each input location")
                .hasArgs()
                .create(Feature.CMD_ARG_INPUT_FILE));
        opts.addOption(OptionBuilder.withArgName(Feature.CMD_ARG_OUTPUT_DIR)
                .isRequired()
                .withDescription("output location")
                .hasArg()
                .create(Feature.CMD_ARG_OUTPUT_DIR));
        opts.addOption(OptionBuilder.withArgName(Feature.CMD_ARG_INPUT_DIR)
                .withDescription("a directory from which we will recursively pull sstables")
                .hasArgs()
                .create(Feature.CMD_ARG_INPUT_DIR));
        opts.addOption(OptionBuilder.withArgName(Feature.CMD_ARG_PRODUCE_SSTABLE)
                .withDescription("produces sstable output (default is to produce json)")
                .create(Feature.CMD_ARG_PRODUCE_SSTABLE));
        opts.addOption(OptionBuilder.withArgName(Feature.CMD_ARG_SSTABLE_OUTPUT_VERSION)
                .withDescription("version of sstable to produce (default is to produce " +
                        Descriptor.Version.current_version
                        + ")")
                .hasArg()
                .create(Feature.CMD_ARG_SSTABLE_OUTPUT_VERSION));
        CommandLineParser parser = new GnuParser();

        try {
            CommandLine cl = parser.parse(opts, args, true);
            if (!(cl.hasOption(Feature.CMD_ARG_INPUT_FILE) || cl.hasOption(Feature.CMD_ARG_INPUT_DIR))) {
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

        // Check all of the paths and load the sstable version from the input filenames
        List<Path> paths = Lists.newArrayList();
        if (cl.hasOption(Feature.CMD_ARG_INPUT_FILE)) {
            for (String input : cl.getOptionValues(Feature.CMD_ARG_INPUT_FILE)) {
                paths.add(new Path(input));
            }
        }
        if (cl.hasOption(Feature.CMD_ARG_INPUT_DIR)) {
            paths.addAll(getDataFiles(job.getConfiguration(), cl.getOptionValue(Feature.CMD_ARG_INPUT_DIR)));
        }

        // At this point we have the version of sstable that we can use for this run
        Descriptor.Version version = Descriptor.Version.CURRENT;
        if (cl.hasOption(Feature.CMD_ARG_SSTABLE_OUTPUT_VERSION)) {
            version = new Descriptor.Version(cl.getOptionValue(Feature.CMD_ARG_SSTABLE_OUTPUT_VERSION));
        }
        job.getConfiguration().set(Feature.CONF_SSTABLE_VERSION, version.toString());

        if (job.getConfiguration().get(Feature.CONF_CQL_SCHEMA) != null) {
            setConfigurationFromCql(job.getConfiguration());
        }

        job.setInputFormatClass(AegisthusInputFormat.class);
        job.setMapOutputKeyClass(AegisthusKey.class);
        job.setMapOutputValueClass(AtomWritable.class);
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(RowWritable.class);
        job.setMapperClass(AegisthusKeyMapper.class);
        job.setReducerClass(CassSSTableReducer.class);
        job.setGroupingComparatorClass(AegisthusKeyGroupingComparator.class);
        job.setPartitionerClass(AegisthusKeyPartitioner.class);
        job.setSortComparatorClass(AegisthusKeySortingComparator.class);

        TextInputFormat.setInputPaths(job, paths.toArray(new Path[paths.size()]));

        if (cl.hasOption(Feature.CMD_ARG_PRODUCE_SSTABLE)) {
            job.setOutputFormatClass(SSTableOutputFormat.class);
        } else {
            job.setOutputFormatClass(JsonOutputFormat.class);
        }
        CustomFileNameFileOutputFormat.setOutputPath(job, new Path(cl.getOptionValue(Feature.CMD_ARG_OUTPUT_DIR)));

        job.submit();
        System.out.println(job.getJobID());
        System.out.println(job.getTrackingURL());
        boolean success = job.waitForCompletion(true);

        if (success) {
            Counter errorCounter = job.getCounters().findCounter("aegisthus", "error_skipped_input");
            long errorCount = errorCounter != null ? errorCounter.getValue() : 0L;
            int maxAllowed = job.getConfiguration().getInt(Feature.CONF_MAX_CORRUPT_FILES_TO_SKIP, 0);
            if (errorCounter != null && errorCounter.getValue() > maxAllowed) {
                LOG.error("Found {} corrupt files which is greater than the max allowed {}", errorCount, maxAllowed);
                success = false;
            } else if (errorCount > 0) {
                LOG.warn("Found {} corrupt files but not failing the job because the max allowed is {}",
                        errorCount, maxAllowed);
            }
        }

        return success ? 0 : 1;
    }

    public static final class Feature {
        public static final String CMD_ARG_INPUT_DIR = "inputDir";
        public static final String CMD_ARG_INPUT_FILE = "input";
        public static final String CMD_ARG_OUTPUT_DIR = "output";
        public static final String CMD_ARG_PRODUCE_SSTABLE = "produceSSTable";
        public static final String CMD_ARG_SSTABLE_OUTPUT_VERSION = "sstable_output_version";

        /**
         * If set this is the blocksize aegisthus will use when splitting input files otherwise the hadoop vaule will
         * be used.
         */
        public static final String CONF_BLOCKSIZE = "aegisthus.blocksize";
        /**
         * The column type, used for sorting columns in all output formats and also in the JSON output format. The
         * default is BytesType.
         */
        public static final String CONF_COLUMNTYPE = "aegisthus.columntype";
        /**
         * The converter to use for the column value, used in the JSON output format. The default is BytesType.
         */
        public static final String CONF_COLUMN_VALUE_TYPE = "aegisthus.column_value_type";
        /**
         * Name of the keyspace and dataset to use for the output sstable file name. The default is "keyspace-dataset".
         */
        public static final String CONF_DATASET = "aegisthus.dataset";
        /**
         * The converter to use for the key, used in the JSON output format. The default is BytesType.
         */
        public static final String CONF_KEYTYPE = "aegisthus.keytype";
        /**
         * Earlier versions of Aegisthus did extra formatting on just the column name.  This defaults to false.
         */
        public static final String CONF_LEGACY_COLUMN_NAME_FORMATTING = "aegisthus.legacy_column_name_formatting";
        /**
         * If set rows with columns larger than this size will be dropped during the reduce stage.
         * For legacy reasons this is based on the size of the columns on disk in SSTable format not the string size of
         * the columns.
         */
        public static final String CONF_MAXCOLSIZE = "aegisthus.maxcolsize";
        /**
         * The maximum number of corrupt files that Aegisthus can automatically skip.  Defaults to 0.
         */
        public static final String CONF_MAX_CORRUPT_FILES_TO_SKIP = "aegisthus.max_corrupt_files_to_skip";
        /**
         * Sort the columns by name rather than by the order in Cassandra.  This defaults to false.
         */
        public static final String CONF_SORT_COLUMNS_BY_NAME = "aegisthus.sort_columns_by_name";
        /**
         * The version of SSTable to input and output.
         */
        public static final String CONF_SSTABLE_VERSION = "aegisthus.version_of_sstable";
        /**
         * Configures if the System.exit should be called to end the processing in main.  Defaults to true.
         */
        public static final String CONF_SYSTEM_EXIT = "aegisthus.exit";
        /**
         * The CQL "Create Table" statement that defines the schema of the input sstables.
         */
        public static final String CONF_CQL_SCHEMA = "aegisthus.cql_schema";
    }
}
