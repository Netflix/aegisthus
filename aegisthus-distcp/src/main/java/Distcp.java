import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.Lists;
import com.netflix.aegisthus.tools.DirectoryWalker;
import com.netflix.aegisthus.tools.StorageHelper;
import com.netflix.aegisthus.tools.Utils;

import com.netflix.hadoop.output.CleanOutputFormat;

public class Distcp extends Configured implements Tool {
	public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
		private long count = 0;

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(new LongWritable(count++), value);
		}
	}

	public static class Partition extends Partitioner<LongWritable, Text> {

		@Override
		public int getPartition(LongWritable arg0, Text arg1, int arg2) {
			return Long.valueOf(arg0.get()).intValue() % arg2;
		}
	}

	public static class Reduce extends Reducer<LongWritable, Text, Text, Text> {

		@Override
		protected void reduce(LongWritable key, Iterable<Text> values, Context ctx) throws IOException,
				InterruptedException {
			StorageHelper helper = new StorageHelper(ctx);
			boolean snappy = ctx.getConfiguration().getBoolean(CFG_PREFIX + OPT_PRIAM, false);
			String base = ctx.getConfiguration().get(CFG_PREFIX + OPT_RECURSIVE);
			if (base != null && !base.endsWith("/")) {
				base = base + "/";
			}
			for (Text file : values) {
				String fileString = cleanS3(file.toString().trim());
				if (base == null) {
					helper.copyToTemp(fileString, snappy);
				} else {
					String prefix = fileString.substring(base.length());
					prefix = prefix.replaceAll("/[^/]+$", "");
					helper.copyToTemp(fileString, prefix, snappy);
				}

			}
		}
	}

	private static final String OPT_DISTCP_TARGET = "distcp.target";
	private static final String OPT_INPUT_FILE = "input";
	private static final String OPT_MANIFEST_OUT = "manifestOut";
	private static final String OPT_MANIFEST_IN = "manifest";
	private static final String OPT_OVERWRITE = "overwrite";
	private static final String OPT_PRIAM = "priam";
	private static final String OPT_RECURSIVE = "recursive";
	private static final String CFG_PREFIX = "distcp.";

	private static final Log LOG = LogFactory.getLog(Distcp.class);

	private static final int MAX_REDUCERS = 800;

	private static final String OUTPUT = "output";

	@SuppressWarnings("static-access")
	public static CommandLine getOptions(String[] args) {
		Options opts = new Options();
		opts.addOption(OptionBuilder
				.withArgName(OPT_INPUT_FILE)
				.withDescription("Each input location")
				.hasArgs()
				.create(OPT_INPUT_FILE));
		opts.addOption(OptionBuilder
				.withArgName(OPT_PRIAM)
				.withDescription("the input is snappy stream compressed and should be decompressed (priam backup)")
				.create(OPT_PRIAM));
		opts.addOption(OptionBuilder
				.withArgName(OPT_RECURSIVE)
				.withDescription("retain directory structure under this directory")
				.hasArg()
				.create(OPT_RECURSIVE));
		opts.addOption(OptionBuilder
				.withArgName(OPT_MANIFEST_IN)
				.withDescription("a manifest of the files to be copied")
				.hasArg()
				.create(OPT_MANIFEST_IN));
		opts.addOption(OptionBuilder
				.withArgName(OPT_MANIFEST_IN)
				.withDescription("a manifest of the files to be copied")
				.hasArg()
				.create(OPT_MANIFEST_IN));
		opts.addOption(OptionBuilder
				.withArgName(OPT_MANIFEST_OUT)
				.withDescription("write out a manifest file of movement")
				.create(OPT_MANIFEST_OUT));
		opts.addOption(OptionBuilder
				.withArgName(OPT_OVERWRITE)
				.withDescription("overwrite the target directory if it exists.")
				.create(OPT_OVERWRITE));
		opts.addOption(OptionBuilder
				.withArgName(OUTPUT)
				.isRequired()
				.withDescription("output location")
				.hasArg()
				.create(OUTPUT));
		CommandLineParser parser = new GnuParser();

		try {
			CommandLine cl = parser.parse(opts, args, true);
			if (cl.hasOption(OPT_MANIFEST_IN) && cl.hasOption(OPT_INPUT_FILE)) {
				System.out.println("Cannot have both a manifest and input files");
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp(String.format("hadoop jar distcp.jar %s", Distcp.class.getName()), opts);
				return null;
			}
			return cl;
		} catch (ParseException e) {
			System.out.println("Unexpected exception:" + e.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(String.format("hadoop jar distcp.jar %s", Distcp.class.getName()), opts);
			return null;
		}

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Distcp(), args);

		System.exit(res);
	}

	/**
	 * checks to see if the output directory exists and throws an error if it
	 * does.
	 * 
	 * TODO: extend this to allow overwrite if set.
	 * 
	 * @throws IOException
	 */
	protected void checkOutputDirectory(Job job, String outputDir, boolean overwrite) throws IOException {
		Path out = new Path(outputDir);
		FileSystem fsOut = out.getFileSystem(job.getConfiguration());
		if (fsOut.exists(out)) {
			if (overwrite) {
				fsOut.delete(out, true);
			} else {
				String error = String.format("Ouput directory (%s) exists, failing", outputDir);
				LOG.error(error);
				throw new IOException(error);
			}
		}
	}

	protected List<FileStatus> getInputs() {
		return null;
	}

	protected Job initializeJob() throws IOException {
		Job job = new Job(getConf());
		job.setJarByClass(Distcp.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(CleanOutputFormat.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setPartitionerClass(Partition.class);
		StorageHelper sh = new StorageHelper(job.getConfiguration());
		LOG.info(String.format("temp location for job: %s", sh.getBaseTempLocation()));

		return job;
	}

	protected static final String cleanS3(String file) {
		return file.replaceFirst("s3://", "s3n://");
	}

	protected void setupTempDirs() {
	}

	protected void setReducers(Job job, int fileCount) {
		int reducers = job.getConfiguration().getInt("mapred.reduce.tasks", 1);
		LOG.info(String.format("fileCount: %d - set reducers: %d", fileCount, reducers));
		if (reducers == 1) {
			job.getConfiguration().setInt("mapred.reduce.tasks", Math.min(fileCount, MAX_REDUCERS));
		} else {
			job.getConfiguration().setInt("mapred.reduce.tasks", Math.min(fileCount, reducers));
		}
	}

	protected int setupInput(Job job, Path inputPath, String[] inputFiles, String manifestPath) throws IOException {
		int size = 0;
		if (manifestPath == null) {
			LOG.info("Setting up input");
			FileSystem fs = inputPath.getFileSystem(job.getConfiguration());
			DataOutputStream dos = fs.create(inputPath);

			List<String> inputs = new ArrayList<>();
			for (int i = 0; i < inputFiles.length; i++) {
				Path path = new Path(cleanS3(inputFiles[i]));
				FileStatus[] files = path.getFileSystem(job.getConfiguration()).globStatus(path);
				for (int j = 0; j < files.length; j++) {
					inputs.add(files[j].getPath().toString());
				}
			}
			List<FileStatus> files = Lists.newArrayList(DirectoryWalker
					.with(job.getConfiguration())
					.addAll(inputs)
					.statuses());

			for (FileStatus file : files) {
				Path filePath = file.getPath();

				// Secondary indexes have the form <keyspace>-<table>.<index_name>-jb-4-Data.db
				// while normal files have the form <keyspace>-<table>-jb-4-Data.db .
				if (filePath.getName().split("\\.").length > 2) {
					LOG.info("Skipping path " + filePath + " as it appears to be a secondary index");
					continue;
				}

				dos.writeBytes(file.getPath().toUri().toString());
				dos.write('\n');
				size = size + 1;
			}
			dos.close();
		} else {
			Utils.copy(new Path(manifestPath), inputPath, false, job.getConfiguration());
			FileSystem fs = inputPath.getFileSystem(job.getConfiguration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inputPath)));
			String l;
			while ((l = br.readLine()) != null) {
				LOG.info(String.format("inputfile: %s", l));
				size++;
			}
		}
		return size;
	}

	@Override
	public int run(String[] args) throws Exception {

		CommandLine cl = getOptions(args);
		if (cl == null) {
			return 1;
		}

		Job job = initializeJob();
		String outputDir = cl.getOptionValue(OUTPUT);
		StorageHelper helper = new StorageHelper(job.getConfiguration());
		helper.setFinalPath(outputDir);

		checkOutputDirectory(job, outputDir, cl.hasOption(OPT_OVERWRITE));

		job.getConfiguration().setBoolean(CFG_PREFIX + OPT_PRIAM, cl.hasOption(OPT_PRIAM));
		if (cl.hasOption(OPT_RECURSIVE)) {
			job.getConfiguration().set(CFG_PREFIX + OPT_RECURSIVE, cleanS3(cl.getOptionValue(OPT_RECURSIVE)));
		}

		String pathTemp = String.format("/tmp/%s", UUID.randomUUID().toString());
		LOG.info(String.format("writing to %s", pathTemp));

		Path tmp = new Path("/tmp");
		FileSystem fs = tmp.getFileSystem(job.getConfiguration());
		fs.mkdirs(new Path(pathTemp));
		Path inputPath = new Path(new Path(pathTemp), "input.txt");
		Path tmpPath = new Path(new Path(pathTemp), "out");

		int fileCount = setupInput(	job,
									inputPath,
									cl.getOptionValues(OPT_INPUT_FILE),
									cl.getOptionValue(OPT_MANIFEST_IN));
		setReducers(job, fileCount);

		TextInputFormat.setInputPaths(job, inputPath.toUri().toString());
		FileOutputFormat.setOutputPath(job, tmpPath);

		boolean success = runJob(job, cl);
		// TODO: output manifest
		/*
		 * if (success && cl.hasOption(OPT_MANIFEST_OUT)) { writeManifest(job,
		 * files); }
		 */
		fs.delete(new Path(pathTemp), true);
		return success ? 0 : 1;
	}

	protected boolean runJob(Job job, CommandLine cl) throws IOException, InterruptedException, ClassNotFoundException {
		job.submit();
		System.out.println(job.getJobID());
		System.out.println(job.getTrackingURL());
		return job.waitForCompletion(true);
	}

	protected void writeManifest(Job job, List<FileStatus> files) throws IOException {
		Path out = new Path(job.getConfiguration().get(OPT_DISTCP_TARGET));
		FileSystem fsOut = out.getFileSystem(job.getConfiguration());
		DataOutputStream dos = fsOut.create(new Path(out, "_manifest/.manifest"));
		for (FileStatus file : files) {
			Path output = new Path(out, file.getPath().getName());
			dos.writeBytes(output.toUri().toString());
			dos.write('\n');
		}
		dos.close();
	}
}
