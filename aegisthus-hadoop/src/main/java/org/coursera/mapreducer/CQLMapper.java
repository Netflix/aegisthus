package org.coursera.mapreducer;

import com.netflix.aegisthus.io.writable.AegisthusKey;
import com.netflix.aegisthus.io.writable.AtomWritable;
import com.netflix.aegisthus.util.CFMetadataUtility;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.CFDefinition;
import org.apache.cassandra.cql3.statements.ColumnGroupMap;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Date;
import java.util.UUID;

public class CQLMapper extends Mapper<AegisthusKey, AtomWritable, AvroKey<GenericRecord>, NullWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(CQLMapper.class);

    ColumnGroupMap.Builder cgmBuilder;
    CFMetaData cfMetaData;
    CFDefinition cfDef;
    ByteBuffer currentKey;

    Schema avroSchema;

    @Override protected void setup(
            Context context)
            throws IOException, InterruptedException {
        avroSchema = AvroJob.getOutputKeySchema(context.getConfiguration());

        cfMetaData = CFMetadataUtility.initializeCfMetaData(context.getConfiguration());
        cfDef = cfMetaData.getCfDef();
        initBuilder();

        /* This exporter assumes tables are composite, which should be true of all current schemas */
        if (!cfDef.isComposite) throw new RuntimeException("Only can export composite CQL table schemas.");
    }

    @Override protected void map(AegisthusKey key, AtomWritable value,
            Context context)
            throws IOException, InterruptedException {
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

        ColumnGroupMap staticGroup = ColumnGroupMap.EMPTY;
        if (!cgmBuilder.isEmpty() && cgmBuilder.firstGroup().isStatic) {
            staticGroup = cgmBuilder.firstGroup();
            cgmBuilder.discardFirst();

            // Special case: if there are no rows, but only the static values, just flush the static values.
            if (cgmBuilder.isEmpty()) {
                handleGroup(context, ColumnGroupMap.EMPTY, keyComponents, staticGroup);
            }
        }

        for (ColumnGroupMap group : cgmBuilder.groups()) {
            handleGroup(context, group, keyComponents, staticGroup);
        }

        initBuilder();
        currentKey = null;
    }

    private void handleGroup(Context context, ColumnGroupMap group, ByteBuffer[] keyComponents, ColumnGroupMap staticGroup)
            throws IOException, InterruptedException {
        GenericRecord record = new GenericData.Record(avroSchema);

        // write out partition keys
        for (CFDefinition.Name name : cfDef.partitionKeys()) {
            addCqlValueToRecord(record, name, keyComponents[name.position]);
        }

        // write out clustering columns
        for (CFDefinition.Name name : cfDef.clusteringColumns()) {
            addCqlValueToRecord(record, name, group.getKeyComponent(name.position));
        }

        // regular columns
        for (CFDefinition.Name name : cfDef.regularColumns()) {
            addValue(record, name, group);
        }

        // static columns
        for (CFDefinition.Name name : cfDef.staticColumns()) {
            addValue(record, name, staticGroup);
        }

        context.write(new AvroKey(record), NullWritable.get());
    }

    /* adapted from org.apache.cassandra.cql3.statements.SelectStatement.addValue */
    private void addValue(GenericRecord record, CFDefinition.Name name, ColumnGroupMap group) {
        if (name.type.isCollection()) {
            // TODO(danchia): support collections
            throw new RuntimeException("Collections not supported yet.");
        } else {
            Column c = group.getSimple(name.name.key);
            addCqlValueToRecord(record, name, (c == null) ? null : c.value());
        }
    }

    private void addCqlValueToRecord(GenericRecord record, CFDefinition.Name name, ByteBuffer value) {
        if (value == null) {
            record.put(name.name.toString(), null);
            return;
        }

        AbstractType<?> type = name.type;
        Object valueDeserialized = type.compose(value);

        AbstractType<?> baseType = (type instanceof ReversedType<?>)
                ? ((ReversedType<?>) type).baseType
                : type;

        /* special case some unsupported CQL3 types to Hive types. */
        if (baseType instanceof UUIDType || baseType instanceof TimeUUIDType) {
            valueDeserialized = ((UUID) valueDeserialized).toString();
        } else if (baseType instanceof BytesType) {
            ByteBuffer buffer = (ByteBuffer) valueDeserialized;
            byte[] data = new byte[buffer.remaining()];
            buffer.get(data);

            valueDeserialized = data;
        } else if (baseType instanceof TimestampType) {
            Date date = (Date) valueDeserialized;
            valueDeserialized = date.getTime();
        }

        //LOG.info("Setting {} type {} to class {}", name.name.toString(), type, valueDeserialized.getClass());

        record.put(name.name.toString(), valueDeserialized);
    }
}
