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
package com.netflix.aegisthus.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.google.common.collect.Lists;

/**
 * A small tool for recursing directories in Hadoop.
 */
public class DirectoryWalker implements Cloneable {
	private static class Cache implements Runnable {
		public static Cache with(DirectoryWalker dw) {
			return new Cache(dw);
		}

		private DirectoryWalker dw;
		private List<PartitionInfo> files;

		private PartitionInfo partitionInfo;

		protected Cache(DirectoryWalker dw) {
			this.dw = dw;
		}

		public Cache cache(PartitionInfo partitionInfo) {
			this.partitionInfo = partitionInfo;
			return this;
		}

		public Cache into(List<PartitionInfo> files) {
			this.files = files;
			return this;
		}

		public void run() {
			try {
				List<PartitionInfo> files = null;
				if (partitionInfo.getStatus(dw.conf) != null) {
					files = Lists.newArrayList(dw.add(partitionInfo).partitionInfo());
					if (files.size() > 0) {
						LOG.info(String.format("%s : % 4d file(s)", partitionInfo
								.getStatus(dw.conf)
								.getPath()
								.toUri()
								.toString(), files.size()));
					}
				}
				try {
					dw.lock.lock();
					this.files.addAll(files);
				} finally {
					dw.lock.unlock();
				}
			} catch (IOException e) {
				LOG.warn(String.format("%s : doesn't exist", partitionInfo
						.getStatus(dw.conf)
						.getPath()
						.toUri()
						.toString()));
				throw new RuntimeException(e);
			}
		}
	}

	public static class PartitionInfo {
		private String location;
		private String partitionName;
		private FileStatus status;

		public PartitionInfo(FileStatus status) {
			this.status = status;
		}

		public PartitionInfo(String partitionName) {
			this.partitionName = partitionName;
		}

		public PartitionInfo(String partitionName, FileStatus status) {
			this.partitionName = partitionName;
			this.status = status;
		}

		public PartitionInfo(String partitionName, String location) {
			this.partitionName = partitionName;
			this.setLocation(location);
		}

		public String getLocation() {
			return location;
		}

		public String getPartitionName() {
			return partitionName;
		}

		public FileStatus getStatus(Configuration conf) {
			if (status == null && location != null) {
				Path path = new Path(location);
				try {
					FileSystem fs = path.getFileSystem(conf);
					status = fs.getFileStatus(path);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
			return status;
		}

		public void setLocation(String location) {
			this.location = location;
		}

		public void setPartitionName(String partitionName) {
			this.partitionName = partitionName;
		}

		public void setStatus(FileStatus status) {
			this.status = status;
		}
	}

	public static final Pattern batch = Pattern.compile("batch_?id=[0-9]+/?$");

	public static final PathFilter HIDDEN_FILE_FILTER = new PathFilter() {
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".");
		}
	};

	private static final Log LOG = LogFactory.getLog(DirectoryWalker.class);

	private static FileStatus[] filterBatch(FileStatus[] files, boolean batched) {
		if (batched && files.length > 0 && batch.matcher(files[0].getPath().toString()).find()) {
			FileStatus first = files[0];
			FileStatus last = files[files.length - 1];
			if (first.getPath().toUri().toString().compareTo(last.getPath().toUri().toString()) > 0) {
				return new FileStatus[] { first };
			}
			return new FileStatus[] { last };
		}
		return files;
	}

	public static DirectoryWalker with(Configuration conf) {
		return new DirectoryWalker(conf);
	}
	private String base = null;
	private boolean batched = false;
	private final Configuration conf;

	private ExecutorService es = null;
	private List<PartitionInfo> files = Lists.newArrayList();
	public ChainedPathFilter filter = new ChainedPathFilter();
	private FileSystem fs;
	protected Lock lock = new ReentrantLock();
	private boolean manifest = false;
	private boolean omitHidden = true;
	private boolean onlyOne = false;
	private boolean recursive = true;
	private boolean stopAdding = false;

	private boolean threaded = false;

	protected DirectoryWalker(Configuration conf) {
		this.conf = conf;
	}

	public DirectoryWalker add(FileStatus status) throws IOException {
		this.fs = status.getPath().getFileSystem(conf);
		this.base = status.getPath().toUri().toString();
		if (!base.endsWith("/")) {
			base = base + "/";
		}
		process(new PartitionInfo(status));
		return this;
	}

	public DirectoryWalker add(PartitionInfo partitionInfo) throws IOException {
		this.base = partitionInfo.getStatus(conf).getPath().toUri().toString();
		if (!base.endsWith("/")) {
			base = base + "/";
		}
		this.fs = partitionInfo.getStatus(conf).getPath().getFileSystem(conf);
		process(partitionInfo);
		return this;
	}

	public DirectoryWalker add(Path path) throws IOException {
		this.base = path.toUri().toString();
		if (!base.endsWith("/")) {
			base = base + "/";
		}
		this.fs = path.getFileSystem(conf);
		process(new PartitionInfo(fs.getFileStatus(path)));
		return this;
	}

	public DirectoryWalker add(String location) throws IOException {
		if (stopAdding) {
			return this;
		}
		Path path = new Path(location);
		fs = path.getFileSystem(conf);
		this.base = location;
		if (!base.endsWith("/")) {
			base = base + "/";
		}
		if (fs.exists(path)) {
			FileStatus status = fs.getFileStatus(path);
			process(new PartitionInfo(status));
		} else {
			LOG.warn(String.format("%s does not exist", location));
		}
		return this;
	}

	public DirectoryWalker addAll(List<String> locations) throws IOException {
		if (stopAdding) {
			return this;
		}
		for (String location : locations) {
			if (threaded) {
				// We do the thread here to get around having to do a
				// getFileStatus on each string in the main thread,
				// which is slow for a large number of partitions on s3.
				try {
					es.submit(Cache
							.with((DirectoryWalker) this.clone())
							.into(this.files)
							.cache(new PartitionInfo(null, location)));
				} catch (CloneNotSupportedException e) {
				}
			} else {
				add(location);
			}
		}
		return this;
	}

	public DirectoryWalker addAllPartitions(List<PartitionInfo> partitions) throws IOException {
		if (stopAdding) {
			return this;
		}
		for (PartitionInfo partitionInfo : partitions) {
			process(partitionInfo);
		}
		return this;
	}

	public DirectoryWalker addAllStatuses(List<FileStatus> locations) throws IOException {
		if (stopAdding) {
			return this;
		}
		for (FileStatus status : locations) {
			process(new PartitionInfo(status));
		}
		return this;
	}

	private void awaitTermination() {
		if (threaded && !es.isShutdown()) {
			es.shutdown();
			try {
				es.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
			} catch (InterruptedException e) {
			}
		}
	}

	public DirectoryWalker batched(boolean batched) {
		this.batched = batched;
		return this;
	}

	protected boolean cache(PartitionInfo partitionInfo) throws IOException {
		FileStatus status = partitionInfo.getStatus(conf);
		if (manifest && status.isDir()) {
			Path manifest = new Path(status.getPath(), "_manifest/_manifest");
			if (fs.exists(manifest)) {
				LOG.info(String.format("Using manifest for partition %s", partitionInfo.partitionName));
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(manifest)));
				String file;
				while ((file = br.readLine()) != null) {
					files.add(new PartitionInfo(partitionInfo.getPartitionName(), fs.getFileStatus(new Path(file))));
				}
				return true;
			}
		}
		if (status.isDir()) {
			if (filter.accept(status.getPath())) {
				for (FileStatus file : filterBatch(fs.listStatus(status.getPath()), batched)) {
					if (!omitHidden || HIDDEN_FILE_FILTER.accept(file.getPath())) {
						PartitionInfo child = new PartitionInfo(partitionInfo.getPartitionName(), file);
						if ((recursive || !file.isDir()) && !cache(child)) {
							return false;
						}
					}
				}
			}
		} else {
			files.add(partitionInfo);
			stopAdding = onlyOne;
			return !onlyOne;
		}
		return true;
	}

	public DirectoryWalker clearFilter() {
		filter.clear();
		return this;
	}

	@Override
	protected Object clone() throws CloneNotSupportedException {
		DirectoryWalker dw = DirectoryWalker.with(conf);
		dw.onlyOne = this.onlyOne;
		dw.omitHidden = this.omitHidden;
		dw.stopAdding = this.stopAdding;
		dw.batched = this.batched;
		dw.recursive = this.recursive;
		dw.filter = this.filter;
		dw.lock = this.lock;
		dw.manifest = this.manifest;
		return dw;
	}

	public DirectoryWalker filter(PathFilter filter) {
		this.filter.add(filter);
		return this;
	}

	public DirectoryWalker manifest(boolean manifest) {
		this.manifest = manifest;
		return this;
	}

	public DirectoryWalker omitHidden() {
		omitHidden = true;
		return this;
	}

	public DirectoryWalker omitHidden(boolean omit) {
		omitHidden = omit;
		return this;
	}

	/**
	 * This will stop the walk of the directory when we find a single file that
	 * matches the filter
	 */
	public DirectoryWalker onlyOne() {
		onlyOne = true;
		return this;
	}

	public DirectoryWalker onlyOne(boolean onlyOne) {
		this.onlyOne = onlyOne;
		return this;
	}

	public List<PartitionInfo> partitionInfo() {
		awaitTermination();
		return files;
	}

	public Iterable<Path> paths() {
		awaitTermination();
		List<Path> paths = Lists.newArrayList();
		for (PartitionInfo partitionInfo : files) {
			paths.add(partitionInfo.getStatus(conf).getPath());
		}
		return paths;
	}

	public Iterable<String> pathsString() {
		awaitTermination();
		List<String> paths = Lists.newArrayList();
		for (PartitionInfo partitionInfo : files) {
			paths.add(partitionInfo.getStatus(conf).getPath().toUri().getPath());
		}
		return paths;
	}

	protected boolean process(PartitionInfo partitionInfo) throws IOException {
		if (threaded) {
			try {
				es.submit(Cache.with((DirectoryWalker) this.clone()).into(this.files).cache(partitionInfo));
			} catch (CloneNotSupportedException e) {
			}
			return true;
		} else {
			return cache(partitionInfo);
		}
	}

	public DirectoryWalker recursive(boolean recursive) {
		this.recursive = recursive;
		return this;
	}

	/**
	 * Example:<br/>
	 * DirectoryWalker.with(conf).add("/my/path").relativePathStrings();<br/>
	 * will return the relative paths of all the files under /my/path
	 * 
	 * @return the relative paths in a directory.
	 */
	public Iterable<String> relativePathStrings() {
		awaitTermination();
		if (base == null) {
			throw new RuntimeException("relativePathStrings will only work with a single base using add");
		}
		List<String> paths = Lists.newArrayList();
		for (PartitionInfo partitionInfo : files) {
			FileStatus status = partitionInfo.getStatus(conf);
			String path = status.getPath().toUri().toString();
			if (!path.contains(base)) {
				throw new RuntimeException("relativePathStrings will only work with a single base using add");
			}
			LOG.info(path);
			LOG.info(base);
			path = path.substring(path.indexOf(base) + base.length());
			try {
				path = URLDecoder.decode(path, "UTF-8");
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
			LOG.info(path);
			paths.add(path);
		}
		return paths;
	}

	public Iterable<FileStatus> statuses() {
		awaitTermination();
		List<FileStatus> statuses = Lists.newArrayList();
		for (PartitionInfo partitionInfo : files) {
			statuses.add(partitionInfo.getStatus(conf));
		}
		return statuses;
	}

	public DirectoryWalker threaded() {
		return threaded(25);
	}

	public DirectoryWalker threaded(int threads) {
		this.es = Executors.newFixedThreadPool(threads);
		this.threaded = true;
		return this;
	}
}
