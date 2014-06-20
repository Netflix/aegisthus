package com.netflix.hadoop.output;


import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.netflix.aegisthus.tools.StorageHelper;


public class CleanOutputCommitter extends OutputCommitter {
	private static final Log LOG = LogFactory.getLog(CleanOutputCommitter.class);;

	@Override
	public void abortTask(TaskAttemptContext ctx) throws IOException {
		// NOOP - everything will be cleaned up by the job at the end.
		// Things that are written out will be handled by future attempts
	}

	@Override
	public void abortJob(JobContext job, State state) throws IOException {
		LOG.info("aborting job");
		StorageHelper sh = new StorageHelper(job.getConfiguration());
		try {
			LOG.info("deleting committed files");
			sh.deleteCommitted();
		} finally {
			LOG.info("deleting temp files");
			sh.deleteBaseTempLocation();
		}
	}

	@Override
	public void commitJob(JobContext job) throws IOException {
		LOG.info("committing job");
		StorageHelper sh = new StorageHelper(job.getConfiguration());
		sh.deleteBaseTempLocation();
	}

	@Override
	public void commitTask(TaskAttemptContext ctx) throws IOException {
		LOG.info("committing task");
		StorageHelper sh = new StorageHelper(ctx);
		sh.moveTaskOutputToFinal();
	}

	@Override
	public boolean needsTaskCommit(TaskAttemptContext ctx) throws IOException {
		if (ctx.getTaskAttemptID().isMap()) {
			return false;
		}
		return true;
	}

	@Override
	public void setupJob(JobContext job) throws IOException {
		StorageHelper sh = new StorageHelper(job.getConfiguration());
		LOG.info(String.format("temp location for job: %s", sh.getBaseTempLocation()));
	}

	@Override
	public void setupTask(TaskAttemptContext ctx) throws IOException {
		StorageHelper sh = new StorageHelper(ctx);
		LOG.info(String.format("temp location for task: %s", sh.getBaseTaskAttemptTempLocation()));
	}
}
