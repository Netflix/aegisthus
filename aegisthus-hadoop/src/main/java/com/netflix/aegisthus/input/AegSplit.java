/**
 * Copyright 2013 Netflix, Inc.
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
package com.netflix.aegisthus.input;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;

import com.google.common.collect.Maps;
import com.netflix.aegisthus.io.sstable.compression.CompressionInputStream;
import com.netflix.aegisthus.io.sstable.compression.CompressionMetadata;

@SuppressWarnings("rawtypes")
public class AegSplit extends InputSplit implements Writable {
	private static final Log LOG = LogFactory.getLog(AegSplit.class);

	public enum Type {
		commitlog, json, sstable
	}

	protected Map<String, AbstractType> convertors = null;
	protected boolean compressed;
	protected Path compressedPath;
	protected long end;
	protected String[] hosts;
	protected Path path;
	protected long start;
	protected Type type;
    protected CompressionMetadata compressionMetadata = null;

	public AegSplit() {
	}

	public AegSplit(Path path,
					long start,
					long length,
					String[] hosts,
					Map<String, AbstractType> convertors,
					boolean compressed,
					Path compressedPath) {
		this(path, start, length, hosts, convertors);
		this.compressed = compressed;
		this.compressedPath = compressedPath;
	}

	public AegSplit(Path path, long start, long length, String[] hosts, Map<String, AbstractType> convertors) {
		this.type = Type.sstable;
		this.path = path;
		this.start = start;
		this.end = length + start;
		LOG.info(String.format("start: %d, end: %d", start, end));
		this.hosts = hosts;
		this.convertors = convertors;
	}

	public AegSplit(Path path, long start, long length, String[] hosts, Type type, Map<String, AbstractType> convertors) {
		this.type = type;
		this.path = path;
		this.start = start;
		this.end = length + start;
		LOG.info(String.format("start: %d, end: %d", start, end));
		this.hosts = hosts;
		this.convertors = convertors;
	}

	public AegSplit(Path path, long start, long length, String[] hosts, Type type) {
		this.type = type;
		this.path = path;
		this.start = start;
		this.end = length + start;
		LOG.info(String.format("start: %d, end: %d", start, end));
		this.hosts = hosts;
	}

	public Path getCompressedPath() {
		return compressedPath;
	}

	public Map<String, AbstractType> getConvertors() {
		return convertors;
	}

	public long getEnd() {
		return end;
	}

	@Override
	public long getLength() {
		return end - start;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return hosts;
	}

	public Path getPath() {
		return path;
	}

	public long getStart() {
		return start;
	}

	public Type getType() {
		return type;
	}

	public boolean isCompressed() {
		return compressed;
	}
	
	public CompressionMetadata getCompressionMetadata() {
		return compressionMetadata;
	}

	public InputStream getInput(Configuration conf) throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		FSDataInputStream fileIn = fs.open(path);
		InputStream dis = new DataInputStream(new BufferedInputStream(fileIn));
		if (compressed) {
			FSDataInputStream cmIn = fs.open(compressedPath);
			compressionMetadata = new CompressionMetadata(new BufferedInputStream(cmIn), end - start);
			dis = new CompressionInputStream(dis, compressionMetadata);
			end = compressionMetadata.getDataLength();
		}
		return dis;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		end = in.readLong();
		hosts = WritableUtils.readStringArray(in);
		path = new Path(WritableUtils.readString(in));
		compressed = in.readBoolean();
		if (compressed) {
			compressedPath = new Path(WritableUtils.readString(in));
		}
		start = in.readLong();
		type = WritableUtils.readEnum(in, Type.class);
		int size = in.readInt();
		if (type == Type.sstable) {
			convertors = Maps.newHashMap();
			for (int i = 0; i < size; i++) {
				String[] parts = WritableUtils.readStringArray(in);
				try {
					convertors.put(parts[0], TypeParser.parse(parts[1]));
				} catch (ConfigurationException e) {
					throw new IOException(e);
				} catch (SyntaxException e) {
					throw new IOException(e);
                }
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(end);
		WritableUtils.writeStringArray(out, hosts);
		WritableUtils.writeString(out, path.toUri().toString());
		out.writeBoolean(compressed);
		if (compressed) {
			WritableUtils.writeString(out, compressedPath.toUri().toString());
		}
		out.writeLong(start);
		WritableUtils.writeEnum(out, type);
		if (convertors != null) {
			String[] parts = new String[2];
			out.writeInt(convertors.size());
			for (Map.Entry<String, AbstractType> e : convertors.entrySet()) {
				parts[0] = e.getKey();
				parts[1] = e.getValue().toString();
				WritableUtils.writeStringArray(out, parts);
			}
		} else {
			out.writeInt(0);
		}
	}

}
