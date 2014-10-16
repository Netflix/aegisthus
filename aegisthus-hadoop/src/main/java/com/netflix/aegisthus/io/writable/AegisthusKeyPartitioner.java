package com.netflix.aegisthus.io.writable;

import org.apache.hadoop.mapreduce.Partitioner;

public class AegisthusKeyPartitioner extends Partitioner<AegisthusKey, AtomWritable> {
    @Override
    public int getPartition(AegisthusKey key, AtomWritable value, int numPartitions) {
        return Math.abs(key.getKey().hashCode() % numPartitions);
    }
}
