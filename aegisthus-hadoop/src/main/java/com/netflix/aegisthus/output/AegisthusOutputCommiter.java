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
package com.netflix.aegisthus.output;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class AegisthusOutputCommiter extends OutputCommitter {
    public static final PathFilter hiddenFileFilter = new PathFilter() {
        public boolean accept(Path p) {
            String name = p.getName();
            return !name.startsWith("_") && !name.startsWith(".");
        }
    };
    private static final Log LOG = LogFactory.getLog(AegisthusOutputCommiter.class);
    private String finalLocation;
    private String tempDir;
    private String extension;

    public AegisthusOutputCommiter(TaskAttemptContext ctx, String finalLocation, String tempDir, String extension) {
        this.finalLocation = finalLocation;
        this.tempDir = tempDir;
        this.extension = extension;
    }

    public static boolean delete(String location, Configuration conf) throws IOException {
        Path filePath = new Path(location);
        FileSystem fsOut = filePath.getFileSystem(conf);
        int tries = 0;
        while (tries++ < 3) {
            try {
                LOG.info("deleting:" + filePath.toString());
                if (!fsOut.delete(filePath, false)) {
                    throw new RuntimeException("Failed to delete");
                }
                return true;
            } catch (Exception e) {
                LOG.info("deleting failed:" + e.getMessage());
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e1) {
                }
            }
        }
        return false;
    }

    public static void copy(Path from, Path to, TaskAttemptContext ctx) throws IOException {
        for (int tries = 0; tries < 3; tries++) {
            FileSystem fromFs = from.getFileSystem(ctx.getConfiguration());
            FileSystem toFs = to.getFileSystem(ctx.getConfiguration());

            InputStream in = fromFs.open(from);
            OutputStream out = toFs.create(to, true);

            byte[] buffer = new byte[65536];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) > 0) {
                out.write(buffer, 0, bytesRead);
                ctx.progress();
            }
            in.close();
            out.close();

            for (int i = 0; i < 5; i++) {
                if (toFs.exists(to)) {
                    return;
                }
                LOG.info(String.format("%s was not found", to.toUri().toString()));
                try {
                    Thread.sleep(i * 1000);
                } catch (InterruptedException e) {
                    // Ignore
                }
            }
        }
        throw new IOException(String.format("%s was not found", to.toUri().toString()));
    }

    @Override
    public void cleanupJob(JobContext job) throws IOException {
        Path path = new Path(tempDir);
        FileSystem fs = path.getFileSystem(job.getConfiguration());
        fs.delete(path, true);
    }

    @Override
    public void abortTask(TaskAttemptContext ctx) throws IOException {
        if (ctx.getTaskAttemptID().isMap()) {
            return;
        }
        LOG.info("***********abortTask");
    }

    @Override
    public void commitTask(TaskAttemptContext ctx) throws IOException {
        if (ctx.getTaskAttemptID().isMap()) {
            return;
        }
        LOG.info("***********commitTask");
        Path temp = new Path(tempDir, getTempFilename(ctx));
        Path finalP = new Path(finalLocation, getFinalFilename(ctx));
        LOG.info(String.format("copying %s to %s", temp.toUri().toString(), finalP.toUri().toString()));
        copy(temp, finalP, ctx);
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext ctx) throws IOException {
        if (ctx.getTaskAttemptID().isMap()) {
            return false;
        }
        return true;
    }

    @Override
    public void abortJob(JobContext job, State state) throws IOException {
        LOG.info("***********abortJob");
        Path outputPath = new Path(finalLocation);
        FileSystem fs = outputPath.getFileSystem(job.getConfiguration());
        fs.delete(outputPath, true);
    }

    @Override
    public void commitJob(JobContext job) throws IOException {
        LOG.info("***********commitJob");
        Path outputPath = new Path(finalLocation);
        FileSystem fs = outputPath.getFileSystem(job.getConfiguration());
        int expected = Integer.parseInt(job.getConfiguration().get("mapred.reduce.tasks"));
        FileStatus[] files = fs.listStatus(outputPath, hiddenFileFilter);
        LOG.info(String.format("%s - %d (expected %d) files", outputPath, files.length, expected));
/*		if (files.length != expected) {
            throw new IOException(String.format("%s - %d (expected %d) files", outputPath, files.length, expected));
		}*/

        super.commitJob(job);
    }

    @Override
    public void setupJob(JobContext arg0) throws IOException {
    }

    @Override
    public void setupTask(TaskAttemptContext arg0) throws IOException {

    }

    public String getFinalFilename(TaskAttemptContext ctx) {
        return String.format("aeg-%05d%s", ctx.getTaskAttemptID().getTaskID().getId(), extension);
    }

    public String getTempFilename(TaskAttemptContext ctx) {
        return String.format("aeg-%05d-%03d%s", ctx.getTaskAttemptID().getTaskID().getId(), ctx
                .getTaskAttemptID()
                .getId(), extension);
    }

}
