package com.netflix.aegisthus.output;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.netflix.Aegisthus;
import com.netflix.aegisthus.io.writable.RowWritable;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.io.sstable.Descriptor.Version;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;

import java.io.IOException;
import java.text.NumberFormat;

public class SSTableOutputFormat extends CustomFileNameFileOutputFormat<BytesWritable, RowWritable> implements
        Configurable {
    private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();

    static {
        NUMBER_FORMAT.setMinimumIntegerDigits(10);
        NUMBER_FORMAT.setGroupingUsed(false);
    }

    private Configuration configuration;
    private Version version;

    @Override
    public Configuration getConf() {
        return configuration;
    }

    @Override
    public void setConf(Configuration conf) {
        this.configuration = conf;
        String sstableVersion = conf.get(Aegisthus.Feature.CONF_SSTABLE_VERSION);
        Preconditions.checkState(!Strings.isNullOrEmpty(sstableVersion), "SSTable version is required configuration");
        this.version = new Version(sstableVersion);

    }

    @Override
    public synchronized String getCustomFileName(TaskAttemptContext context, String name, String extension) {
        TaskID taskId = context.getTaskAttemptID().getTaskID();
        int partition = taskId.getId();
        String sstableVersion = context.getConfiguration().get(Aegisthus.Feature.CONF_SSTABLE_VERSION);
        return context.getConfiguration().get(Aegisthus.Feature.CONF_DATASET, "keyspace-dataset")
                + "-" + sstableVersion
                + "-" + NUMBER_FORMAT.format(partition)
                + "-Data.db";
    }

    @Override
    public RecordWriter<BytesWritable, RowWritable> getRecordWriter(TaskAttemptContext context) throws IOException {
        // No extension on the aeg json format files for historical reasons
        Path workFile = getDefaultWorkFile(context, "");
        FileSystem fs = workFile.getFileSystem(context.getConfiguration());
        final FSDataOutputStream fileOut = fs.create(workFile, false);
        final OnDiskAtom.Serializer serializer = OnDiskAtom.Serializer.instance;

        return new RecordWriter<BytesWritable, RowWritable>() {
            @Override
            public void write(BytesWritable key, RowWritable rowWritable) throws IOException, InterruptedException {
                if (version.hasRowSizeAndColumnCount) {
                    writeVersion_1_2_5(key, rowWritable);
                } else {
                    writeVersion_2_0(key, rowWritable);
                }
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                fileOut.close();
            }

            void writeVersion_1_2_5(BytesWritable key, RowWritable row) throws IOException {
                fileOut.writeShort(key.getLength());
                fileOut.write(key.getBytes());

                long dataSize = 16; // The bytes for the Int, Long, Int after this loop
                for (OnDiskAtom atom : row.getColumns()) {
                    dataSize += atom.serializedSizeForSSTable();
                }
                fileOut.writeLong(dataSize);
                fileOut.writeInt((int) (row.getDeletedAt() / 1000));
                fileOut.writeLong(row.getDeletedAt());
                fileOut.writeInt(row.getColumns().size());
                for (OnDiskAtom atom : row.getColumns()) {
                    serializer.serializeForSSTable(atom, fileOut);
                }
            }

            void writeVersion_2_0(BytesWritable key, RowWritable row) throws IOException {
                fileOut.writeShort(key.getLength());
                fileOut.write(key.getBytes());

                fileOut.writeInt((int) (row.getDeletedAt() / 1000));
                fileOut.writeLong(row.getDeletedAt());
                for (OnDiskAtom atom : row.getColumns()) {
                    serializer.serializeForSSTable(atom, fileOut);
                }
                fileOut.writeShort(SSTableWriter.END_OF_ROW);
            }
        };
    }
}
