package org.coursera.mapreducer;

import com.netflix.aegisthus.io.writable.AegisthusKey;
import com.netflix.aegisthus.io.writable.AtomWritable;
import com.netflix.aegisthus.util.CFMetadataUtility;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.CFDefinition;
import org.apache.cassandra.cql3.statements.ColumnGroupMap;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class CQLMapper extends Mapper<AegisthusKey, AtomWritable, Text, Text> {
    private static final Logger LOG = LoggerFactory.getLogger(CQLMapper.class);

    ColumnGroupMap.Builder cgmBuilder;
    CFMetaData cfMetaData;
    CFDefinition cfDef;
    ByteBuffer currentKey = null;

    @Override protected void setup(
            Context context)
            throws IOException, InterruptedException {
        cfMetaData = CFMetadataUtility.initializeCfMetaData(context.getConfiguration());
        cfDef = cfMetaData.getCfDef();
        initBuilder();

        /* This exporter assumes tables are composite, which should be true of all current schemas */
        if (!cfDef.isComposite) throw new RuntimeException("Only can export composite CQL table schemas.");
    }

    @Override protected void map(AegisthusKey key, AtomWritable value,
            Context context)
            throws IOException, InterruptedException {
        LOG.info("Looking at key {}", ByteBufferUtil.string(key.getKey()));

        if (currentKey == null) {
            currentKey = key.getKey();
        } else if (!currentKey.equals(key.getKey())) {
            flushCgm(context);
            currentKey = key.getKey();
        }

        OnDiskAtom atom = value.getAtom();
        if (atom == null) {
            LOG.warn("Got null atom for key {}.", cfMetaData.getKeyValidator().compose(key.getKey()));
            return;
        }

        if (atom instanceof Column) {
            cgmBuilder.add((Column) atom);
        } else {
            LOG.error("Non-colum atom. {} {}", atom.getClass(), atom);
            throw new IllegalArgumentException("Got a non-column Atom.");
        }
    }

    @Override protected void cleanup(
            Context context)
            throws IOException, InterruptedException {
        super.cleanup(context);

        if (currentKey != null) {
            flushCgm(context);
        }
    }

    private void initBuilder() {
        // TODO: we might need to make "current" time configurable to avoid wrongly expiring data when trying to backfill.
        cgmBuilder = new ColumnGroupMap.Builder((CompositeType) cfMetaData.comparator,
                cfDef.hasCollections, System.currentTimeMillis());
    }

    private void flushCgm(Context context) throws IOException, InterruptedException {
        if (cgmBuilder.isEmpty())
            return;

        ByteBuffer[] keyComponents =
                cfDef.hasCompositeKey
                        ? ((CompositeType) cfMetaData.getKeyValidator()).split(currentKey)
                        : new ByteBuffer[] { currentKey };

        // TODO(danchia): handle static groups

        for (ColumnGroupMap group : cgmBuilder.groups()) {
            handleGroup(context, group, keyComponents);
        }

        initBuilder();
        currentKey = null;
    }

    private void handleGroup(Context context, ColumnGroupMap group, ByteBuffer[] keyComponents)
            throws IOException, InterruptedException {
        StringBuilder strBuilder = new StringBuilder();

        // write out partition keys
        for (CFDefinition.Name name : cfDef.partitionKeys()) {
            strBuilder.append("[")
                    .append(name.name.toString())
                    .append(":")
                    .append(name.type.compose(keyComponents[name.position]))
                    .append("]");
        }

        strBuilder.append("|");

        // write out clustering columns
        for (CFDefinition.Name name : cfDef.clusteringColumns()) {
            strBuilder.append("[")
                    .append(name.name.toString())
                    .append(":")
                    .append(name.type.compose(group.getKeyComponent(name.position)))
                    .append("]");
        }

        strBuilder.append("|");

        // regular columns
        for (CFDefinition.Name name : cfDef.regularColumns()) {
            addValue(strBuilder, name, group);
        }

        context.write(new Text(""), new Text(strBuilder.toString()));
    }

    /* adapted from org.apache.cassandra.cql3.statements.SelectStatement.addValue */
    private void addValue(StringBuilder strBuilder, CFDefinition.Name name, ColumnGroupMap group) {
        String value = "NULL";

        if (group == null) {
            value = "NULL";
        } else if (name.type.isCollection()) {
            // TODO(danchia): support collections
            throw new RuntimeException("Collections not supported yet.");
        } else {
            Column c = group.getSimple(name.name.key);
            if (c != null) {
                value = name.type.compose(c.value()).toString();
            }
        }

        strBuilder.append("[")
                .append(name.name.toString())
                .append(":")
                .append(value)
                .append("]");
    }
}
