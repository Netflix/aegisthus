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
package com.netflix.aegisthus.pig;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.rmi.UnexpectedException;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.apache.pig.LoadCaster;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AegisthusLoadCaster implements LoadCaster {
	private static Logger LOG = LoggerFactory.getLogger(AegisthusLoadCaster.class);
	private static final Hex hex = new Hex();

	@Override
	public DataBag bytesToBag(byte[] arg0, ResourceFieldSchema arg1) throws IOException {
		throw new IOException("Don't do that");
	}

	@Override
	public String bytesToCharArray(byte[] arg0) throws IOException {
		if (arg0 == null || arg0.length == 0) {
			return null;
		}
		try {
			return new String(hex.decode(arg0));
		} catch (Exception e) {
			LOG.error("failed to convert " + new String(arg0) + " to chararray");
			return null;
		}
	}

	@Override
	public Double bytesToDouble(byte[] arg0) throws IOException {
		if (arg0 == null || arg0.length == 0) {
			return null;
		}
		try {
			byte[] by = hex.decode(arg0);
			ByteBuffer bb = ByteBuffer.allocate(by.length);
			bb.put(by);
			bb.position(0);
			return bb.getDouble();
		} catch (Exception e) {
			LOG.error("failed to convert " + new String(arg0) + " to double");
			return null;
		}
	}

	@Override
	public Float bytesToFloat(byte[] arg0) throws IOException {
		if (arg0 == null || arg0.length == 0) {
			return null;
		}
		try {
			byte[] by = hex.decode(arg0);
			ByteBuffer bb = ByteBuffer.allocate(by.length);
			bb.put(by);
			bb.position(0);
			return bb.getFloat();
		} catch (Exception e) {
			LOG.error("failed to convert " + new String(arg0) + " to float");
			return null;
		}
	}

	@Override
	public Integer bytesToInteger(byte[] arg0) throws IOException {
		if (arg0 == null || arg0.length == 0) {
			return null;
		}
		try {
			return Integer.valueOf(bytesToCharArray(arg0));
		} catch (Exception e) {
		}
		try {
			return (int) getNumber(arg0);
		} catch (Exception e) {
			LOG.error("failed to convert " + new String(arg0) + " to int");
			return null;
		}
	}
	
	private long getNumber(byte[] arg0) throws Exception {
		byte[] by = hex.decode(arg0);
		ByteBuffer bb = ByteBuffer.allocate(by.length);
		bb.put(by);
		bb.position(0);
		switch(by.length) {
		case 1:
			return (long)bb.get();
		case 2:
			return (long)bb.getShort();
		case 4:
			return (long)bb.getInt();
		case 8:
			return (long)bb.getLong();
		}
		throw new UnexpectedException("couldn't determine datatype");
	}

	@Override
	public Long bytesToLong(byte[] arg0) throws IOException {
		if (arg0 == null || arg0.length == 0) {
			return null;
		}
		try {
			return Long.valueOf(bytesToCharArray(arg0));
		} catch (Exception e) {
		}
		try {
			return getNumber(arg0);
		} catch (Exception e) {
			LOG.error("failed to convert " + new String(arg0) + " to long");
			return null;
		}
	}

	@Override
    @Deprecated
	public Map<String, Object> bytesToMap(byte[] arg0) throws IOException {
		throw new IOException("Don't do that");
	}

	@Override
	public Tuple bytesToTuple(byte[] arg0, ResourceFieldSchema arg1) throws IOException {
		throw new IOException("Don't do that");
	}

	@Override
	public Map<String, Object> bytesToMap(byte[] arg0, ResourceFieldSchema arg1) throws IOException {
		throw new IOException("Don't do that");
	}

	@Override
	public Boolean bytesToBoolean(byte[] arg0) throws IOException {
		throw new IOException("Doesn't handle boolean");
	}

	@Override
	public DateTime bytesToDateTime(byte[] arg0) throws IOException {
		throw new IOException("Doesn't handle DateTime");
	}
}
