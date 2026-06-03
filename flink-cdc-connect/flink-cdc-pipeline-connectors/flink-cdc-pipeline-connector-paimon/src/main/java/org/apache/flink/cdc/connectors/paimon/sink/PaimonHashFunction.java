/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.paimon.sink;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.function.HashFunction;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.paimon.sink.utils.PaimonCatalogRetryUtils;
import org.apache.flink.cdc.connectors.paimon.sink.v2.PaimonWriterHelper;

import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.sink.RowAssignerChannelComputer;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A {@link HashFunction} implementation for {@link PaimonDataSink}. Shuffle {@link DataChangeEvent}
 * by hash of PrimaryKey.
 *
 * <p>The catalog lookup is deferred until {@link #hashcode(DataChangeEvent)} is called for the
 * first time and uses a bounded retry. This avoids querying the Paimon catalog inside the
 * constructor (which is invoked by {@code RegularPrePartitionOperator} the moment it forwards a
 * {@code SchemaChangeEvent} downstream). At that point the {@code PaimonMetadataApplier} may not
 * yet have created the physical table (e.g. when JM/TM use independent filesystem catalogs, or
 * because {@code applyCreateTable} is racing with the broadcast), so transient catalog visibility
 * issues no longer crash the job.
 */
public class PaimonHashFunction implements HashFunction<DataChangeEvent>, Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(PaimonHashFunction.class);

    private final Options options;
    private final TableId tableId;
    private final Schema schema;
    private final ZoneId zoneId;
    private final int parallelism;

    // Initialized lazily on the first hashcode() call to avoid querying the Paimon catalog
    // before PaimonMetadataApplier has created the physical table.
    private transient volatile boolean initialized;
    private transient List<RecordData.FieldGetter> fieldGetters;
    private transient RowAssignerChannelComputer channelComputer;

    public PaimonHashFunction(
            Options options, TableId tableId, Schema schema, ZoneId zoneId, int parallelism) {
        this.options = options;
        this.tableId = tableId;
        this.schema = schema;
        this.zoneId = zoneId;
        this.parallelism = parallelism;
    }

    private void ensureInitialized() {
        if (initialized) {
            return;
        }
        synchronized (this) {
            if (initialized) {
                return;
            }
            FileStoreTable table = lookupTableWithRetry();
            if (table instanceof AppendOnlyFileStore) {
                this.fieldGetters = null;
                this.channelComputer = null;
            } else {
                this.fieldGetters = PaimonWriterHelper.createFieldGetters(schema, zoneId);
                this.channelComputer = new RowAssignerChannelComputer(table.schema(), parallelism);
                this.channelComputer.setup(parallelism);
            }
            this.initialized = true;
        }
    }

    private FileStoreTable lookupTableWithRetry() {
        Catalog catalog = FlinkCatalogFactory.createPaimonCatalog(options);
        try {
            return (FileStoreTable)
                    PaimonCatalogRetryUtils.getTableWithRetry(
                            catalog,
                            Identifier.fromString(tableId.toString()),
                            "PaimonHashFunction");
        } finally {
            try {
                catalog.close();
            } catch (Exception e) {
                LOG.debug("Failed to close Paimon catalog after table lookup.", e);
            }
        }
    }

    @Override
    public int hashcode(DataChangeEvent event) {
        ensureInitialized();
        if (channelComputer != null) {
            GenericRow genericRow =
                    PaimonWriterHelper.convertEventToGenericRow(event, fieldGetters);
            return channelComputer.channel(genericRow);
        } else {
            // Avoid sending all events to the same subtask when table has no primary key.
            return ThreadLocalRandom.current().nextInt(parallelism);
        }
    }
}
