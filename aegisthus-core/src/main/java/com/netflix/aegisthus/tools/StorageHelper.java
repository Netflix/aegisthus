/**
 * Copyright 2014 Netflix, Inc.
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
package com.netflix.aegisthus.tools;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class StorageHelper {
	public static String CFG_BASE_TEMP_DIR = "storagehelper.base.temp.dir";
	public static String CFG_OUTPUT_DIR = "storagehelper.output.dir";
	public static String CFG_STORAGE_DEBUG = "storagehelper.debug";

	private static final Log LOG = LogFactory.getLog(StorageHelper.class);;
	private Configuration config = null;
	private TaskAttemptContext ctx = null;
	private boolean debug = false;
	private Set<String> jobFiles = null;
	private Set<String> taskFiles = null;

	public StorageHelper(Configuration config) {
		this.config = config;
		debug = config.getBoolean(CFG_STORAGE_DEBUG, false);
	}

	public StorageHelper(TaskAttemptContext ctx) {
		this.ctx = ctx;
		this.config = ctx.getConfiguration();
		debug = config.getBoolean(CFG_STORAGE_DEBUG, false);
	}

	private Path commitPath() {
		String base = getBaseTempLocation();
		return new Path(base, "commits");
	}

	private Path commitPath(int taskId) {
		Path commits = commitPath();
		return new Path(commits, String.format("commit-%d", taskId));
	}

	public void copyToTemp(String file, String prefix, boolean snappy) throws IOException {
		String target = getBaseTaskAttemptTempLocation();
		Path targetPath = new Path(target, prefix);
		Path filePath = new Path(file);
		Path fullPath = new Path(targetPath, filePath.getName());

		String log = String.format("copying %s to %s", file, fullPath.toUri().toString());
		LOG.info(log);
		ctx.setStatus(log);
		Utils.copy(new Path(file), fullPath, snappy, ctx);
	}

	public void copyToTemp(String file, boolean snappy) throws IOException {
		String target = getBaseTaskAttemptTempLocation();
		Path targetPath = new Path(target);
		Path filePath = new Path(file);
		Path fullPath = new Path(targetPath, filePath.getName());
		String log = String.format("copying %s to %s", file, fullPath.toUri().toString());
		LOG.info(log);
		ctx.setStatus(log);
		Utils.copy(filePath, fullPath, snappy, ctx);
	}

	public void deleteBaseTempLocation() throws IOException {
		String base = getBaseTempLocation();
		Path path = new Path(base);
		FileSystem fs = path.getFileSystem(config);
		LOG.info(String.format("deleting: %s", base));
		fs.delete(path, true);
	}

	public void deleteCommitted() throws IOException {
		for (String file : getCommittedFiles()) {
			LOG.info(String.format("deleting: %s", file));
			Utils.delete(config, new Path(file), false);
		}
	}

	private int getAttemptId() throws IOException {
		if (ctx == null) {
			throw new IOException("Not running in a TaskAttemptContext");
		}
		return ctx.getTaskAttemptID().getId();
	}

	public String getBaseTaskAttemptTempLocation() throws IOException {
		int taskId = getTaskId();
		int attemptId = getAttemptId();
		String base = getBaseTempLocation();
		return String.format("%s/%d-%d", base, taskId, attemptId);

	}

	/**
	 * This method has a side effect of setting values in the config for the
	 * job.
	 */
	public String getBaseTempLocation() {
		String base = config.get(CFG_BASE_TEMP_DIR);
		if (base == null) {
			base = String.format("%s/%s", "/tmp", UUID.randomUUID());
			config.set(CFG_BASE_TEMP_DIR, base);
		}
		return base;
	}

	public Set<String> getCommittedFiles() throws IOException {
		if (jobFiles == null) {
			List<Path> logs = Lists.newArrayList(DirectoryWalker.with(config).add(commitPath()).paths());
			jobFiles = readCommitLogs(logs);
		}
		return jobFiles;
	}

	public Set<String> getCommittedFiles(int taskId) throws IOException {
		if (taskFiles == null) {
			List<Path> logs = Lists.newArrayList();
			logs.add(commitPath(taskId));
			taskFiles = readCommitLogs(logs);
		}
		return taskFiles;
	}

	public List<String> getCommittedFolderList() throws IOException {
		Set<String> folders = Sets.newHashSet();
		Set<String> files = getCommittedFiles();
		for (String file : files) {
			folders.add(file.replaceAll("/[^/]+$", ""));
		}
		return Lists.newArrayList(folders);
	}

	public Path getFinalPath() throws IOException {
		if (config.get(CFG_OUTPUT_DIR) == null) {
			throw new IOException(String.format("%s cannot be null", CFG_OUTPUT_DIR));
		}
		return new Path(config.get(CFG_OUTPUT_DIR));
	}

	private int getTaskId() throws IOException {
		if (ctx == null) {
			throw new IOException("Not running in a TaskAttemptContext");
		}
		return ctx.getTaskAttemptID().getTaskID().getId();
	}

	/**
	 * This method will check if this file was previously committed by this
	 * task.
	 */
	public boolean isFileMine(int taskId, String file) throws IOException {
		return getCommittedFiles(taskId).contains(file);
	}

	public void logCommit(String file) throws IOException {
		Path log = commitPath(getTaskId());
		if (debug) {
			LOG.info(String.format("logging (%s) to commit log (%s)", file, log.toUri().toString()));
		}
		FileSystem fs = log.getFileSystem(config);
		DataOutputStream os = null;
		if (fs.exists(log)) {
			os = fs.append(log);
		} else {
			os = fs.create(log);
		}
		os.writeBytes(file);
		os.write('\n');
		os.close();
	}

	public void moveTaskOutputToFinal() throws IOException {
		String tempLocation = getBaseTaskAttemptTempLocation();
		Path path = new Path(tempLocation);
		List<String> relativePaths = Lists.newArrayList(DirectoryWalker
				.with(config)
				.threaded()
				.omitHidden()
				.add(path)
				.relativePathStrings());
		Path finalPath = getFinalPath();
		for (String relative : relativePaths) {
			LOG.info(String.format("moving (%s) from (%s) to (%s)", relative, path.toUri().toString(), finalPath
					.toUri()
					.toString()));
			Utils.copy(new Path(relative), path, finalPath, ctx);
			ctx.progress();
		}
	}

	private Set<String> readCommitLogs(List<Path> logs) throws IOException {
		Set<String> files = Sets.newHashSet();
		FileSystem fs = null;

		for (Path log : logs) {
			// all logs are on the same filesystem.
			if (fs == null) {
				fs = log.getFileSystem(config);
			}
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(log)));
			String file;
			while ((file = br.readLine()) != null) {
				files.add(file);
			}

			if (ctx != null && files.size() % 1000 == 0) {
				ctx.progress();
			}
			br.close();
		}
		return files;
	}

	public void setFinalPath(String path) {
		config.set(CFG_OUTPUT_DIR, path);
	}
}
