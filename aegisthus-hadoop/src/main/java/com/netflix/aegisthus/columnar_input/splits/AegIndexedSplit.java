package com.netflix.aegisthus.columnar_input.splits;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;

/**
 * The purpose of this split type is to allow the underlying scanner to keep
 * track of the rows while iterating over the split. When we hit corrupt rows,
 * the index can be used to reposition the stream to the correct place.
 **/
public class AegIndexedSplit extends AegSplit {
	private static final Log LOG = LogFactory.getLog(AegIndexedSplit.class);

	private Path indexPath;
	private long indexStart;
	
	public AegIndexedSplit() {}

	public AegIndexedSplit(Path path,
			long start,
			long length,
			String[] hosts,
			Path indexPath,
			long indexStart) {
		super(path, start, length, hosts);
		this.indexPath = indexPath;
		this.indexStart = indexStart;
	}

	public AegIndexedSplit(Path path,
			long start,
			long length,
			String[] hosts,
			Type type,
			Path indexPath,
			long indexStart) {
		super(path, start, length, hosts, type);
		this.indexPath = indexPath;
		this.indexStart = indexStart;
	}

	public InputStream getIndexInput(Configuration conf) throws IOException {
		LOG.info(String.format("using index(%s) at %d", indexPath.toUri().toString(), indexStart));
		FileSystem fs = indexPath.getFileSystem(conf);
		FSDataInputStream fileIn = fs.open(indexPath);
		InputStream dis = new DataInputStream(new BufferedInputStream(fileIn));
		long skipped = 0;
		while (skipped < indexStart) {
            skipped += dis.skip(indexStart - skipped);
		}
		return dis;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		indexPath = new Path(WritableUtils.readString(in));
		indexStart = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		WritableUtils.writeString(out, indexPath.toUri().toString());
		out.writeLong(indexStart);
	}
}
