package com.netflix.aegisthus.input.splits;

import com.netflix.aegisthus.io.sstable.compression.CompressionInputStream;
import com.netflix.aegisthus.io.sstable.compression.CompressionMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;

public class AegCompressedSplit extends AegSplit {
    private static final Logger LOG = LoggerFactory.getLogger(AegCompressedSplit.class);
    private Path compressedPath;
    private CompressionMetadata compressionMetadata = null;

    public static AegCompressedSplit createAegCompressedSplit(@Nonnull Path path,
            long start,
            long length,
            @Nonnull String[] hosts,
            @Nonnull Path compressedPath) {
        AegCompressedSplit split = new AegCompressedSplit();
        split.path = path;
        split.start = start;
        split.end = length + start;
        split.hosts = hosts;
        LOG.info("start: {}, end: {}", start, split.end);
        split.compressedPath = compressedPath;

        return split;
    }

    @Override
    public long getDataEnd() {
        if (compressionMetadata == null) {
            throw new IllegalStateException("getDataEnd was called before getInput");
        }
        return compressionMetadata.getDataLength();
    }

    @Nonnull
    @Override
    public InputStream getInput(@Nonnull Configuration conf) throws IOException {
        FileSystem fs = compressedPath.getFileSystem(conf);
        InputStream dis = super.getInput(conf);
        FSDataInputStream cmIn = fs.open(compressedPath);
        compressionMetadata = new CompressionMetadata(new BufferedInputStream(cmIn), getEnd() - getStart());
        dis = new CompressionInputStream(dis, compressionMetadata);
        end = compressionMetadata.getDataLength();
        return dis;
    }

    @Override
    public void readFields(@Nonnull DataInput in) throws IOException {
        super.readFields(in);
        compressedPath = new Path(WritableUtils.readString(in));
    }

    @Override
    public void write(@Nonnull DataOutput out) throws IOException {
        super.write(out);
        WritableUtils.writeString(out, compressedPath.toUri().toString());
    }
}
