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
    private Path compressionMetadataPath;
    private long compressedLength;

    public static AegCompressedSplit createAegCompressedSplit(@Nonnull Path path, long start, long length,
            @Nonnull String[] hosts, @Nonnull Path compressionMetadataPath, @Nonnull Configuration conf)
            throws IOException {
        AegCompressedSplit split = new AegCompressedSplit();
        split.path = path;
        split.start = start;
        split.compressedLength = length;
        split.hosts = hosts;
        split.compressionMetadataPath = compressionMetadataPath;

        CompressionMetadata compressionMetadata = getCompressionMetadata(conf, compressionMetadataPath, length, false);
        split.end = compressionMetadata.getDataLength();
        LOG.info("start: {}, end: {}", start, split.end);

        return split;
    }

    private static CompressionMetadata getCompressionMetadata(Configuration conf, Path compressionMetadataPath,
            long compressedLength, boolean doNonEndCalculations) throws IOException {
        FileSystem fs = compressionMetadataPath.getFileSystem(conf);
        try (FSDataInputStream cmIn = fs.open(compressionMetadataPath);
                BufferedInputStream inputStream = new BufferedInputStream(cmIn)) {
            return new CompressionMetadata(inputStream, compressedLength, doNonEndCalculations);
        }
    }

    @Nonnull
    @Override
    public InputStream getInput(@Nonnull Configuration conf) throws IOException {
        CompressionMetadata compressionMetadata = getCompressionMetadata(conf, compressionMetadataPath,
                compressedLength, true);
        return new CompressionInputStream(super.getInput(conf), compressionMetadata);
    }

    @Override
    public void readFields(@Nonnull DataInput in) throws IOException {
        super.readFields(in);
        compressionMetadataPath = new Path(WritableUtils.readString(in));
        compressedLength = WritableUtils.readVLong(in);
    }

    @Override
    public void write(@Nonnull DataOutput out) throws IOException {
        super.write(out);
        WritableUtils.writeString(out, compressionMetadataPath.toUri().toString());
        WritableUtils.writeVLong(out, compressedLength);
    }
}
