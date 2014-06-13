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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.xerial.snappy.SnappyInputStream2;

public class Utils {
	private static final Log LOG = LogFactory.getLog(Utils.class);

	public static void copy(Path from, Path to, boolean snappy, Configuration conf) throws IOException {
		FileSystem fromFs = from.getFileSystem(conf);
		FileSystem toFs = to.getFileSystem(conf);

		InputStream in = fromFs.open(from);
		OutputStream out = toFs.create(to, false);
		try {
			if (snappy) {
				in = new SnappyInputStream2(in);
			}
			byte[] buffer = new byte[65536];
			int bytesRead;
			while ((bytesRead = in.read(buffer)) >= 0) {
				if (bytesRead > 0) {
					out.write(buffer, 0, bytesRead);
				}
			}
		} finally {
			in.close();
			out.close();
		}
	}

	public static void copy(Path from, Path to, boolean snappy, TaskAttemptContext ctx) throws IOException {
		FileSystem fromFs = from.getFileSystem(ctx.getConfiguration());
		FileSystem toFs = to.getFileSystem(ctx.getConfiguration());

		if (!to.isAbsolute()) {
			to = new Path(ctx.getConfiguration().get("mapred.working.dir"), to);
		}
		if (!snappy && onSameHdfs(ctx.getConfiguration(), from, to)) {
			LOG.info(String.format("renaming %s to %s", from, to));
			toFs.mkdirs(to.getParent());
			toFs.rename(from, to);
			return;
		}

		InputStream in = fromFs.open(from);
		OutputStream out = toFs.create(to, false);
		try {
			if (snappy) {
				in = new SnappyInputStream2(in);
			}
			byte[] buffer = new byte[65536];
			int bytesRead;
			int count = 0;
			while ((bytesRead = in.read(buffer)) >= 0) {
				if (bytesRead > 0) {
					out.write(buffer, 0, bytesRead);
				}
				if (count++ % 50 == 0) {
					ctx.progress();
				}
			}
		} finally {
			in.close();
			out.close();
		}
	}

	public static void copy(Path relative, Path fromDir, Path toDir, TaskAttemptContext ctx) throws IOException {
		Path from = new Path(fromDir, relative);
		Path to = new Path(toDir, relative);
		copy(from, to, false, ctx);
	}

	public static boolean delete(Configuration config, Path path, boolean recursive) throws IOException {
		FileSystem fs = path.getFileSystem(config);
		return fs.delete(path, recursive);
	}

	protected static boolean onSameHdfs(Configuration conf, Path a, Path b) throws IOException {
		FileSystem aFs = a.getFileSystem(conf);
		FileSystem bFs = b.getFileSystem(conf);

		return (aFs.equals(bFs));
	}

}
