package com.netflix.hadoop.output;

import java.io.IOException;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class CleanOutputFormat<K, V> extends OutputFormat<K, V> {
	public static class CleanOutputRecordWriter<K, V> extends RecordWriter<K, V> {

		@Override
		public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
			// TODO Auto-generated method stub

		}

		@Override
		public void write(K arg0, V arg1) throws IOException, InterruptedException {
			// TODO Auto-generated method stub

		}

	}

	@Override
	public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext arg0) throws IOException {
		return new CleanOutputCommitter();
	}

	@Override
	public void checkOutputSpecs(JobContext arg0) throws IOException, InterruptedException {

	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new CleanOutputRecordWriter<K, V>();
	}

}
