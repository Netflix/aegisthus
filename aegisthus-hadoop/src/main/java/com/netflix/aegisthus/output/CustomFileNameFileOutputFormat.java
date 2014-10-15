package com.netflix.aegisthus.output;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.NumberFormat;

public abstract class CustomFileNameFileOutputFormat<K, V> extends FileOutputFormat<K, V> {
    private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();

    static {
        NUMBER_FORMAT.setMinimumIntegerDigits(5);
        NUMBER_FORMAT.setGroupingUsed(false);
    }

    /**
     * Generate a unique filename, based on the task id, name, and extension
     * @param context the task that is calling this
     * @param name the base filename
     * @param extension the filename extension
     * @return a string like $name-[jobType]-$id$extension
     */
    protected synchronized String getCustomFileName(TaskAttemptContext context,
            String name,
            String extension) {
        TaskID taskId = context.getTaskAttemptID().getTaskID();
        int partition = taskId.getId();
        return name + '-' + NUMBER_FORMAT.format(partition) + extension;
    }

    /**
     * Get the default path and filename for the output format.
     * @param context the task context
     * @param extension an extension to add to the filename
     * @return a full path $output/_temporary/$task-id/part-[mr]-$id
     * @throws java.io.IOException
     */
    @Override
    public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
        FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
        return new Path(committer.getWorkPath(), getCustomFileName(context, getOutputName(context), extension));
    }
}
