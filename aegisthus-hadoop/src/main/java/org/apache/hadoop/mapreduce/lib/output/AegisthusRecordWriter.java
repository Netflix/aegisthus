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
package org.apache.hadoop.mapreduce.lib.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * This class writes our output into dynamic partitioned directories. It can
 * handle speculative execution, but will try to fail if writing over old data
 * (as best it can).
 *
 */
public class AegisthusRecordWriter extends RecordWriter<Text, Text> {
    private static final Log LOG = LogFactory.getLog(AegisthusRecordWriter.class);
    private LineRecordWriter lrw;

    public AegisthusRecordWriter(TaskAttemptContext ctx, CompressionCodec codec, String extension, String tempDir) {
        int taskId = ctx.getTaskAttemptID().getTaskID().getId();
        int taskAttemptId = ctx.getTaskAttemptID().getId();
        try {
            Path path = new Path(String.format("aeg-%05d-%03d%s", taskId, taskAttemptId, extension));
            Path file = new Path(tempDir, path);
            FileSystem fs = file.getFileSystem(ctx.getConfiguration());
            LOG.info(String.format("writing to %s", file.toUri().toString()));
            DataOutputStream fileOut = fs.create(file, false);

            if (codec != null) {
                fileOut = new DataOutputStream(codec.createOutputStream(fileOut));
            }
            lrw = new LineRecordWriter(fileOut, "\t");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close(TaskAttemptContext ctx) throws IOException, InterruptedException {
        lrw.close(ctx);
    }

    @Override
    public void write(Text key, Text value) throws IOException, InterruptedException {
        lrw.write(key, value);
    }

    protected static class LineRecordWriter extends TextOutputFormat.LineRecordWriter<Text, Text> {
        public LineRecordWriter(DataOutputStream arg0, String seperator) {
            super(arg0, seperator);
        }
    }
}
