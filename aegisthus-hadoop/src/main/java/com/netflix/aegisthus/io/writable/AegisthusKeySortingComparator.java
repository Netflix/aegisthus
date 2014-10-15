package com.netflix.aegisthus.io.writable;

import com.netflix.Aegisthus;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class AegisthusKeySortingComparator extends WritableComparator implements Configurable {
    private Comparator<ByteBuffer> comparator;
    private Configuration conf;

    public AegisthusKeySortingComparator() {
        super(AegisthusKey.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {
        AegisthusKey ck1 = (AegisthusKey) wc1;
        AegisthusKey ck2 = (AegisthusKey) wc2;
        ck1.setComparator(comparator);
        return ck1.compareTo(ck2);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        String comparatorType = conf.get(Aegisthus.Feature.CONF_COLUMNTYPE);
        try {
            comparator = TypeParser.parse(comparatorType);
        } catch (SyntaxException | ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }
}
