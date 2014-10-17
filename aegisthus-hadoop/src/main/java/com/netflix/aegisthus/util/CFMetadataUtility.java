package com.netflix.aegisthus.util;

import com.google.common.base.Preconditions;
import com.netflix.Aegisthus;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.hadoop.conf.Configuration;

public class CFMetadataUtility {
    public static CFMetaData initializeCfMetaData(Configuration configuration) {
        final String cql = configuration.get(Aegisthus.Feature.CONF_CQL_SCHEMA);
        Preconditions.checkNotNull(cql, "Cannot proceed without CQL definition.");

        final CreateTableStatement statement = getCreateTableStatement(cql);

        try {
            final CFMetaData cfMetaData = statement.getCFMetaData();
            cfMetaData.rebuild();

            return cfMetaData;
        } catch (RequestValidationException e) {
            // Cannot proceed if an error occurs
            throw new RuntimeException("Error initializing CFMetadata from CQL.", e);
        }
    }

    private static CreateTableStatement getCreateTableStatement(String cql) {
        CreateTableStatement statement;
        try {
            statement = (CreateTableStatement) QueryProcessor.parseStatement(cql).prepare().statement;
        } catch (RequestValidationException e) {
            // Cannot proceed if an error occurs
            throw new RuntimeException("Error configuring SSTable reader. Cannot proceed", e);
        }
        return statement;
    }

}

