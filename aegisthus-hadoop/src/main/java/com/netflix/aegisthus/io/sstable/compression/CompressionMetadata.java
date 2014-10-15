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
package com.netflix.aegisthus.io.sstable.compression;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.FileUtils;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class CompressionMetadata {
    private List<Integer> chunkLengths;
    private int current;
    private long dataLength;
    private CompressionParameters parameters;

    public CompressionMetadata(InputStream compressionInput, long compressedLength) throws IOException {
        DataInputStream stream = new DataInputStream(compressionInput);

        String compressorName = stream.readUTF();
        int optionCount = stream.readInt();
        Map<String, String> options = Maps.newHashMap();
        for (int i = 0; i < optionCount; ++i) {
            String key = stream.readUTF();
            String value = stream.readUTF();
            options.put(key, value);
        }
        int chunkLength = stream.readInt();
        try {
            parameters = new CompressionParameters(compressorName, chunkLength, options);
        } catch (ConfigurationException e) {
            throw new RuntimeException("Cannot create CompressionParameters for stored parameters", e);
        }

        setDataLength(stream.readLong());
        chunkLengths = readChunkLengths(stream, compressedLength);
        current = 0;

        FileUtils.closeQuietly(stream);
    }

    public int chunkLength() {
        return parameters.chunkLength();
    }

    public ICompressor compressor() {
        return parameters.sstableCompressor;
    }

    public int currentLength() {
        if (current < chunkLengths.size()) {
            return chunkLengths.get(current);
        }
        return -1;
    }

    public long getDataLength() {
        return dataLength;
    }

    public void setDataLength(long dataLength) {
        this.dataLength = dataLength;
    }

    public void incrementChunk() {
        current++;
    }

    private List<Integer> readChunkLengths(DataInput input, long compressedLength) throws IOException {
        int chunkCount = input.readInt();
        List<Integer> lengths = Lists.newArrayList();

        long prev = input.readLong();
        for (int i = 1; i < chunkCount; i++) {
            long cur = input.readLong();
            lengths.add((int) (cur - prev - 4));
            prev = cur;
        }
        lengths.add((int) (compressedLength - prev - 4));

        return lengths;
    }
}
