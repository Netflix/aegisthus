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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.AegisthusRecordWriter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class AegisthusOutputFormat extends TextOutputFormat<Text, Text> {
    private static final Log LOG = LogFactory.getLog(AegisthusOutputFormat.class);

    public AegisthusOutputFormat() {
    }

    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext ctx) throws IOException, InterruptedException {
        if (ctx.getTaskAttemptID().isMap()) {
            return super.getRecordWriter(ctx);
        }
        String tempDir = ctx.getConfiguration().get("aeg.temp.dir");
        LOG.info(String.format("%s - tempdir", tempDir));

        boolean isCompressed = getCompressOutput(ctx);
        CompressionCodec codec = null;
        String extension = "";
        if (isCompressed) {
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(ctx, GzipCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, ctx.getConfiguration());
            extension = codec.getDefaultExtension();
        }

        RecordWriter<Text, Text> recordWriter = new AegisthusRecordWriter(ctx, codec, extension, tempDir);

        return recordWriter;
    }

    @Override
    public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext ctx) throws IOException {
        String tempDir = ctx.getConfiguration().get("aeg.temp.dir");
        String location = ctx.getConfiguration().get("mapred.output.dir");

        boolean isCompressed = getCompressOutput(ctx);
        CompressionCodec codec = null;
        String extension = "";
        if (isCompressed) {
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(ctx, GzipCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, ctx.getConfiguration());
            extension = codec.getDefaultExtension();
        }

        return new AegisthusOutputCommiter(ctx, location, tempDir, extension);
    }

}
