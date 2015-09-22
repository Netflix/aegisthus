package com.netflix.aegisthus.io.writable;

import com.google.common.collect.ComparisonChain;
import com.netflix.Aegisthus;
import org.apache.cassandra.db.marshal.AbstractType;
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
    private AbstractType<ByteBuffer> columnNameConverter;
    private Configuration conf;
    private boolean legacyColumnNameFormatting;
    private boolean sortColumnsByName;

    private AegisthusKey key1 = new AegisthusKey();
    private AegisthusKey key2 = new AegisthusKey();

    public AegisthusKeySortingComparator() {
        super(AegisthusKey.class, false);
    }

    public static String legacyColumnNameFormat(String columnName) {
        return columnName.replaceAll("[\\s\\p{Cntrl}]", " ").replace("\\", "\\\\").replace("\"", "\\\"");
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        Comparator<ByteBuffer> nameComparator = columnNameConverter;
        if (sortColumnsByName) {
            nameComparator = new Comparator<ByteBuffer>() {
                @Override
                public int compare(ByteBuffer o1, ByteBuffer o2) {
                    if (o1 == null || o2 == null) {
                        return ComparisonChain.start().compare(o1, o2).result();
                    }

                    String c1Name = columnNameConverter.getString(o1);
                    String c2Name = columnNameConverter.getString(o2);
                    if (legacyColumnNameFormatting) {
                        c1Name = legacyColumnNameFormat(c1Name);
                        c2Name = legacyColumnNameFormat(c2Name);
                    }

                    return c1Name.compareTo(c2Name);
                }
            };
        }

        key1.readFields(b1, s1, l1);
        key2.readFields(b2, s2, l2);

        return key1.compareTo(key2, nameComparator);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        String columnType = conf.get(Aegisthus.Feature.CONF_COLUMNTYPE, "BytesType");
        legacyColumnNameFormatting = conf.getBoolean(Aegisthus.Feature.CONF_LEGACY_COLUMN_NAME_FORMATTING, false);
        sortColumnsByName = conf.getBoolean(Aegisthus.Feature.CONF_SORT_COLUMNS_BY_NAME, false);
        try {
            //noinspection unchecked
            columnNameConverter = (AbstractType<ByteBuffer>) TypeParser.parse(columnType);
        } catch (SyntaxException | ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }
}
