package com.netflix.aegisthus.io.writable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AegisthusKeyGroupingComparator extends WritableComparator {
    public AegisthusKeyGroupingComparator() {
        super(AegisthusKey.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {
        AegisthusKey ck1 = (AegisthusKey) wc1;
        AegisthusKey ck2 = (AegisthusKey) wc2;
        return ck1.compareTo(ck2);
    }
}
