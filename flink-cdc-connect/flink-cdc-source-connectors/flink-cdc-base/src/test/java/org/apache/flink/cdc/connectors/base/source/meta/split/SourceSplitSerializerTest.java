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

package org.apache.flink.cdc.connectors.base.source.meta.split;

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.core.memory.DataOutputSerializer;

import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SourceSplitSerializer}. */
class SourceSplitSerializerTest {

    @Test
    void testStreamSplitRoundTripWithPg10RoutingState() throws Exception {
        SourceSplitSerializer serializer = constructSourceSplitSerializer();
        StreamSplit split = constructStreamSplitWithPg10RoutingState();

        StreamSplit deserialized =
                (StreamSplit)
                        serializer.deserialize(
                                serializer.getVersion(), serializer.serialize(split));

        assertThat(deserialized).isEqualTo(split);
        assertThat(deserialized.getPg10ChildToParentMapping())
                .isEqualTo(split.getPg10ChildToParentMapping());
        assertThat(deserialized.getPg10ParentToChildrenMapping())
                .isEqualTo(split.getPg10ParentToChildrenMapping());
    }

    @Test
    void testDeserializePreviousVersionWithoutPg10RoutingState() throws Exception {
        SourceSplitSerializer serializer = constructSourceSplitSerializer();
        StreamSplit split = constructLegacyStreamSplit();

        StreamSplit deserialized =
                (StreamSplit) serializer.deserialize(6, serializeAsVersion6(split));

        assertThat(deserialized.getStartingOffset()).isEqualTo(split.getStartingOffset());
        assertThat(deserialized.getEndingOffset()).isEqualTo(split.getEndingOffset());
        assertThat(deserialized.getFinishedSnapshotSplitInfos())
                .isEqualTo(split.getFinishedSnapshotSplitInfos());
        assertThat(deserialized.getTableSchemas()).isEqualTo(split.getTableSchemas());
        assertThat(deserialized.getTotalFinishedSplitSize())
                .isEqualTo(split.getTotalFinishedSplitSize());
        assertThat(deserialized.isSuspended()).isEqualTo(split.isSuspended());
        assertThat(deserialized.isSnapshotCompleted()).isEqualTo(split.isSnapshotCompleted());
        assertThat(deserialized.getPg10ChildToParentMapping()).isEmpty();
        assertThat(deserialized.getPg10ParentToChildrenMapping()).isEmpty();
    }

    @Test
    void testStreamSplitRoundTripWithInitializedButEmptyPg10RoutingState() throws Exception {
        SourceSplitSerializer serializer = constructSourceSplitSerializer();
        StreamSplit split = constructStreamSplitWithInitializedEmptyPg10RoutingState();

        StreamSplit deserialized =
                (StreamSplit)
                        serializer.deserialize(
                                serializer.getVersion(), serializer.serialize(split));

        assertThat(deserialized.getPg10ChildToParentMapping()).isEmpty();
        assertThat(deserialized.getPg10ParentToChildrenMapping()).isEmpty();
        assertThat(deserialized.isPg10RoutingStateInitialized()).isTrue();
    }

    private byte[] serializeAsVersion6(StreamSplit split) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(64);
        out.writeInt(2);
        out.writeUTF(split.splitId());
        out.writeUTF("");

        SourceSplitSerializer serializer = constructSourceSplitSerializer();
        serializer.writeOffsetPosition(split.getStartingOffset(), out);
        serializer.writeOffsetPosition(split.getEndingOffset(), out);

        out.writeInt(split.getFinishedSnapshotSplitInfos().size());
        for (FinishedSnapshotSplitInfo splitInfo : split.getFinishedSnapshotSplitInfos()) {
            splitInfo.serialize(out);
        }
        SourceSplitSerializer.writeTableSchemas(split.getTableSchemas(), out);
        out.writeInt(split.getTotalFinishedSplitSize());
        out.writeBoolean(split.isSuspended());
        out.writeBoolean(split.isSnapshotCompleted());
        return out.getCopyOfBuffer();
    }

    private StreamSplit constructStreamSplitWithPg10RoutingState() {
        TableId parent = new TableId(null, "inventory", "products");
        TableId childUk = new TableId(null, "inventory", "products_uk");
        TableId childUs = new TableId(null, "inventory", "products_us");

        Map<TableId, TableId> childToParent = new LinkedHashMap<>();
        childToParent.put(childUk, parent);
        childToParent.put(childUs, parent);

        Map<TableId, List<TableId>> parentToChildren = new LinkedHashMap<>();
        parentToChildren.put(parent, new ArrayList<>(List.of(childUk, childUs)));

        return new StreamSplit(
                "stream-split",
                null,
                null,
                new ArrayList<>(),
                constructTableSchema(),
                0,
                false,
                true,
                childToParent,
                parentToChildren);
    }

    private StreamSplit constructLegacyStreamSplit() {
        return new StreamSplit(
                "stream-split",
                null,
                null,
                new ArrayList<>(),
                constructTableSchema(),
                0,
                false,
                true);
    }

    private StreamSplit constructStreamSplitWithInitializedEmptyPg10RoutingState() {
        return new StreamSplit(
                "stream-split",
                null,
                null,
                new ArrayList<>(),
                constructTableSchema(),
                0,
                false,
                true,
                new LinkedHashMap<>(),
                new LinkedHashMap<>(),
                true);
    }

    private HashMap<TableId, TableChanges.TableChange> constructTableSchema() {
        TableId tableId = constructTableId();
        HashMap<TableId, TableChanges.TableChange> tableSchema = new HashMap<>();
        Tables tables = new Tables();
        Table table = tables.editOrCreateTable(tableId).create();
        TableChanges.TableChange tableChange =
                new TableChanges.TableChange(TableChanges.TableChangeType.CREATE, table);
        tableSchema.put(tableId, tableChange);
        return tableSchema;
    }

    private TableId constructTableId() {
        return new TableId("catalog", "inventory", "products");
    }

    private SourceSplitSerializer constructSourceSplitSerializer() {
        return new SourceSplitSerializer() {
            @Override
            public OffsetFactory getOffsetFactory() {
                return new OffsetFactory() {
                    @Override
                    public Offset newOffset(Map<String, String> offset) {
                        return null;
                    }

                    @Override
                    public Offset newOffset(String filename, Long position) {
                        return null;
                    }

                    @Override
                    public Offset newOffset(Long position) {
                        return null;
                    }

                    @Override
                    public Offset createTimestampOffset(long timestampMillis) {
                        return null;
                    }

                    @Override
                    public Offset createInitialOffset() {
                        return null;
                    }

                    @Override
                    public Offset createNoStoppingOffset() {
                        return null;
                    }
                };
            }
        };
    }
}
