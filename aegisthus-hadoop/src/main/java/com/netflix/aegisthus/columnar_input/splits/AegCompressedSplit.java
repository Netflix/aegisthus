package com.netflix.aegisthus.columnar_input.splits;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;

import com.netflix.aegisthus.io.sstable.compression.CompressionInputStream;
import com.netflix.aegisthus.io.sstable.compression.CompressionMetadata;

public class AegCompressedSplit extends AegSplit {
    protected Path compressedPath;
    protected CompressionMetadata compressionMetadata = null;

    public AegCompressedSplit(Path path,
            long start,
            long length,
            String[] hosts,
            Path compressedPath) {
        super(path, start, length, hosts);
        this.compressedPath = compressedPath;
    }

    public Path getCompressedPath() {
        return compressedPath;
    }

    public CompressionMetadata getCompressionMetadata() {
        return compressionMetadata;
    }

    @Override
    public long getDataEnd() {
        return compressionMetadata.getDataLength();
    }

    @Override
    public InputStream getInput(Configuration conf) throws IOException {
        FileSystem fs = getCompressedPath().getFileSystem(conf);
        InputStream dis = super.getInput(conf);
        FSDataInputStream cmIn = fs.open(compressedPath);
        compressionMetadata = new CompressionMetadata(new BufferedInputStream(cmIn), getEnd() - getStart());
        dis = new CompressionInputStream(dis, compressionMetadata);
        end = compressionMetadata.getDataLength();
        return dis;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        compressedPath = new Path(WritableUtils.readString(in));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        WritableUtils.writeString(out, compressedPath.toUri().toString());
    }
}
