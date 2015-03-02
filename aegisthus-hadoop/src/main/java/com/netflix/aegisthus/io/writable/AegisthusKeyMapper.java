package com.netflix.aegisthus.io.writable;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AegisthusKeyMapper extends Mapper<AegisthusKey, AtomWritable, AegisthusKey, AtomWritable> {
    @Override
    protected void map(AegisthusKey key, AtomWritable value, Context context)
            throws IOException, InterruptedException {
        context.write(key, value);
    }
}
