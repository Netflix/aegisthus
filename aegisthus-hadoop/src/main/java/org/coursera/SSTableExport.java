package org.coursera;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.Aegisthus;
import com.netflix.aegisthus.input.AegisthusInputFormat;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.coursera.mapreducer.CQLMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class SSTableExport extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(SSTableExport.class);

    private Descriptor.Version version;

    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<IntWritable, DefaultHCatRecord, Text, DefaultHCatRecord> {
        @Override protected void reduce(IntWritable key, Iterable<DefaultHCatRecord> values,
                Context context)
                throws IOException, InterruptedException {
            for (DefaultHCatRecord v : values) {
                context.write(null, v);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SSTableExport(), args);

        boolean exit = Boolean.valueOf(System.getProperty(Aegisthus.Feature.CONF_SYSTEM_EXIT, "true"));
        if (exit) {
            System.exit(res);
        } else if (res != 0) {
            throw new RuntimeException("SSTableExport finished with a non-zero exit code: " + res);
        }
    }

    private void checkVersionFromFilename(String filename) {
        Descriptor descriptor = Descriptor.fromFilename(filename);

        if (this.version == null) {
            this.version = descriptor.version;
        } else if (!this.version.equals(descriptor.version)) {
            throw new IllegalStateException("All files must have the same sstable version.  File '" + filename
                    + "' has version '" + descriptor.version + "' and we have already seen a file with version '"
                    + version + "'");
        }
    }

    private void setConfigurationFromCql(Configuration conf) {
        CFMetaData cfMetaData = CFMetadataUtility.initializeCfMetaData(conf);
        String keyType = cfMetaData.getKeyValidator().toString();
        String columnType = cfMetaData.comparator.toString();

        LOG.info("From CQL3, setting keyType({}) and columnType({}).", keyType, columnType);

        conf.set(Aegisthus.Feature.CONF_KEYTYPE, keyType);
        conf.set(Aegisthus.Feature.CONF_COLUMNTYPE, columnType);
    }

    List<Path> getDataFiles(Configuration conf, String dir) throws IOException {
        Set<String> globs = Sets.newHashSet();
        List<Path> output = Lists.newArrayList();
        Path dirPath = new Path(dir);
        FileSystem fs = dirPath.getFileSystem(conf);
        List<FileStatus> input = Lists.newArrayList(fs.listStatus(dirPath));
        for (String path : DirectoryWalker.with(conf).threaded().addAllStatuses(input).pathsString()) {
            if (path.endsWith("-Data.db")) {
                checkVersionFromFilename(path);
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
        opts.addOption(OptionBuilder.withArgName(Feature.CMD_ARG_INPUT_DIR)
                .withDescription("a directory from which we will recursively pull sstables")
                .hasArgs()
                .create(Feature.CMD_ARG_INPUT_DIR));
        opts.addOption(OptionBuilder.withArgName(Feature.CMD_ARG_OUTPUT_TABLE)
                .withDescription("hive table to output to")
                .isRequired()
                .hasArgs()
                .create(Feature.CMD_ARG_OUTPUT_TABLE));
        opts.addOption(OptionBuilder.withArgName(Feature.CMD_ARG_OUTPUT_DATABASE)
                .withDescription("hive database to output to")
                .isRequired()
                .hasArgs()
                .create(Feature.CMD_ARG_OUTPUT_DATABASE));
        CommandLineParser parser = new GnuParser();

        try {
            CommandLine cl = parser.parse(opts, args, true);
            if (!(cl.hasOption(Feature.CMD_ARG_INPUT_FILE) || cl.hasOption(Feature.CMD_ARG_INPUT_DIR))) {
                System.out.println("Must have either an input or inputDir option");
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp(String.format("hadoop jar aegisthus.jar %s", SSTableExport.class.getName()), opts);
                return null;
            }
            return cl;
        } catch (ParseException e) {
            System.out.println("Unexpected exception:" + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(String.format("hadoop jar aegisthus.jar %s", SSTableExport.class.getName()), opts);
            return null;
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());

        job.setJarByClass(SSTableExport.class);
        CommandLine cl = getOptions(args);
        if (cl == null) {
            return 1;
        }

        // Check all of the paths and load the sstable version from the input filenames
        List<Path> paths = Lists.newArrayList();
        if (cl.hasOption(Feature.CMD_ARG_INPUT_FILE)) {
            for (String input : cl.getOptionValues(Feature.CMD_ARG_INPUT_FILE)) {
                checkVersionFromFilename(input);
                paths.add(new Path(input));
            }
        }
        if (cl.hasOption(Feature.CMD_ARG_INPUT_DIR)) {
            paths.addAll(getDataFiles(job.getConfiguration(), cl.getOptionValue(Feature.CMD_ARG_INPUT_DIR)));
        }

        // At this point we have the version of sstable that we can use for this run
        job.getConfiguration().set(Aegisthus.Feature.CONF_SSTABLE_VERSION, version.toString());

        if (job.getConfiguration().get(Aegisthus.Feature.CONF_CQL_SCHEMA) != null) {
            setConfigurationFromCql(job.getConfiguration());
        }

        job.setInputFormatClass(AegisthusInputFormat.class);
        job.setOutputFormatClass(HCatOutputFormat.class);
        job.setMapperClass(CQLMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DefaultHCatRecord.class);
        job.setReducerClass(Reducer.class);

        HCatOutputFormat.setOutput(
                job,
                OutputJobInfo.create(
                        cl.getOptionValue(Feature.CMD_ARG_OUTPUT_DATABASE),
                        cl.getOptionValue(Feature.CMD_ARG_OUTPUT_TABLE),
                        null));
        HCatOutputFormat.setSchema(job, HCatOutputFormat.getTableSchema(job.getConfiguration()));

        TextInputFormat.setInputPaths(job, paths.toArray(new Path[paths.size()]));

        job.submit();
        System.out.println(job.getJobID());
        System.out.println(job.getTrackingURL());
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static final class Feature {
        public static final String CMD_ARG_INPUT_DIR = "inputDir";
        public static final String CMD_ARG_INPUT_FILE = "input";
        public static final String CMD_ARG_OUTPUT_DATABASE = "outputDatabase";
        public static final String CMD_ARG_OUTPUT_TABLE = "outputTable";
    }
}
