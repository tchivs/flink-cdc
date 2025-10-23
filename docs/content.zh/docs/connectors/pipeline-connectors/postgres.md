---
title: "Postgres"
weight: 2
type: docs
aliases:
- /connectors/pipeline-connectors/Postgres
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Postgres Connector

Postgres CDC Pipeline 连接器允许从 Postgres 数据库读取快照数据和增量数据，并提供端到端的整库数据同步能力。 本文描述了如何设置 Postgres CDC Pipeline 连接器。
注意：因为Postgres的wal log日志中展示没有办法解析表结构变更记录，因此Postgres CDC Pipeline Source暂时不支持同步表结构变更。

## 示例

从 Postgres 读取数据同步到 Fluss 的 Pipeline 可以定义如下：

```yaml
source:
   type: posgtres
   name: Postgres Source
   hostname: 127.0.0.1
   port: 5432
   username: admin
   password: pass
   # 需要确保所有的表来自同一个database
   tables: adb.\.*.\.*
   decoding.plugin.name:  pgoutput
   slot.name: pgtest

sink:
  type: fluss
  name: Fluss Sink
  bootstrap.servers: localhost:9123
  # Security-related properties for the Fluss client
  properties.client.security.protocol: sasl
  properties.client.security.sasl.mechanism: PLAIN
  properties.client.security.sasl.username: developer
  properties.client.security.sasl.password: developer-pass

pipeline:
   name: Postgres to Fluss Pipeline
   parallelism: 4
```

## 连接器配置项

<div class="highlight">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left" style="width: 10%">Option</th>
        <th class="text-left" style="width: 8%">Required</th>
        <th class="text-left" style="width: 7%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 65%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>hostname</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td> Postgres 数据库服务器的 IP 地址或主机名。</td>
    </tr>
    <tr>
      <td>port</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">5432</td>
      <td>Integer</td>
      <td>Postgres 数据库服务器的整数端口号。</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接到 Postgres 数据库服务器时要使用的 Postgres 用户的名称。</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接 Postgres 数据库服务器时使用的密码。</td>
    </tr>
    <tr>
      <td>tables</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>需要监视的 Postgres 数据库的表名。表名支持正则表达式，以监视满足正则表达式的多个表。<br>
          需要确保所有的表来自同一个数据库。<br>
          需要注意的是，点号（.）被视为数据库、模式和表名的分隔符。 如果需要在正则表达式中使用点（.）来匹配任何字符，必须使用反斜杠对点进行转义。<br>
          例如，bdb.user_schema_[0-9].user_table_[0-9]+, bdb.schema_\.*.order_\.*</td>
    </tr>
    <tr>
      <td>slot.name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>为从特定插件以流式传输方式获取某个数据库/模式的变更数据，所创建的 Postgre 逻辑解码槽（logical decoding slot）的名称。服务器使用这个槽（slot）将事件流式传输给你要配置的连接器（connector）。
          <br/>复制槽名称必须符合 <a href="https://www.postgresql.org/docs/current/static/warm-standby.html#STREAMING-REPLICATION-SLOTS-MANIPULATION">PostgreSQL 复制插槽的命名规则</a>, 其规则如下: "Each replication slot has a name, which can contain lower-case letters, numbers, and the underscore character."</td>
    </tr>
    <tr>
      <td>decoding.plugin.name</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The name of the Postgres logical decoding plug-in installed on the server. Supported values are decoderbufs and pgoutput.</td>
    </tr> 
    <tr>
      <td>tables.exclude</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>需要排除的 Postgres 数据库的表名，参数会在tables参数后发生排除作用。表名支持正则表达式，以排除满足正则表达式的多个表。<br>
          用法和tables参数相同</td>
    </tr>
     <tr>
      <td>server-time-zone</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>数据库服务器中的会话时区， 例如： "Asia/Shanghai". 
          它控制 Postgres 中的时间戳类型如何转换为字符串。
          更多请参考 <a href="https://debezium.io/documentation/reference/1.9/connectors/postgresql.html#postgresql-data-types"> 这里</a>.
          如果没有设置，则使用ZoneId.systemDefault()来确定服务器时区。
      </td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.chunk.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">8096</td>
      <td>Integer</td>
      <td>表快照的块大小（行数），读取表的快照时，捕获的表被拆分为多个块。</td>
    </tr>
    <tr>
      <td>scan.snapshot.fetch.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1024</td>
      <td>Integer</td>
      <td>读取表快照时每次读取数据的最大条数。</td>
    </tr>
    <tr>
      <td>scan.startup.mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">initial</td>
      <td>String</td>
      <td> Postgres CDC 消费者可选的启动模式，
         合法的模式为 "initial"，"latest-offset"，"committed-offset"和 ""snapshot"。</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.backfill.skip</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>
        是否在快照读取阶段跳过 backfill 。<br>
        如果跳过 backfill ，快照阶段捕获表的更改将在稍后的 wal log 读取阶段被回放，而不是合并到快照中。<br>
        警告：跳过 backfill 可能会导致数据不一致，因为快照阶段发生的某些wal log事件可能会被重放（仅保证 at-least-once ）。
        例如，更新快照阶段已更新的值，或删除快照阶段已删除的数据。这些重放的wal log事件应进行特殊处理。
    </tr>
    <tr>
      <td>scan.lsn-commit.checkpoints-num-delay</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>在开始提交LSN偏移量之前，允许的检查点延迟次数。 <br>
          检查点的 LSN 偏移量将以滚动方式提交，最早的那个检查点标识符将首先从延迟的检查点中提交。
      </td>
    </tr>
    <tr>
      <td>connect.timeout</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">30s</td>
      <td>Duration</td>
      <td>连接器在尝试连接到 Postgres 数据库服务器后超时前应等待的最长时间。该时长不能少于250毫秒。</td>
    </tr>    
    <tr>
      <td>connect.max-retries</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>连接器应重试以建立 Postgres 数据库服务器连接的最大重试次数。</td>
    </tr>
    <tr>
      <td>connection.pool.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">20</td>
      <td>Integer</td>
      <td>连接池大小。</td>
    </tr>
    <tr>
      <td>jdbc.properties.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">20</td>
      <td>String</td>
      <td>传递自定义 JDBC URL 属性的选项。用户可以传递自定义属性，如 'jdbc.properties.useSSL' = 'false'.</td>
    </tr>
    <tr>
      <td>heartbeat.interval</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">30s</td>
      <td>Duration</td>
      <td>用于跟踪最新可用wal commited offset 偏移的发送心跳事件的间隔。</td>
    </tr>
    <tr>
      <td>debezium.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>将 Debezium 的属性传递给 Debezium 嵌入式引擎，该引擎用于从 Postgres 服务器捕获数据更改。
          例如: <code>'debezium.snapshot.mode' = 'never'</code>.
          查看更多关于 <a href="https://debezium.io/documentation/reference/1.9/connectors/postgresql.html"> Debezium 的  Postgres 连接器属性</a></td> 
    </tr>
    <tr>
      <td>scan.incremental.close-idle-reader.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否在快照结束后关闭空闲的 Reader。 此特性需要 flink 版本大于等于 1.14 并且 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' 需要设置为 true。<br>
          若 flink 版本大于等于 1.15，'execution.checkpointing.checkpoints-after-tasks-finish.enabled' 默认值变更为 true，可以不用显式配置 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' = true。</td>
    </tr>
    <tr>
      <td>chunk-meta.group.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>String</td>
      <td>
        分块元数据的组大小，如果元数据大小超过该组大小，则元数据将被划分为多个组。
      </td>
    </tr>
    <tr>
      <td>metadata.list</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>String</td>
      <td>
        源记录中可读取的元数据列表，将传递给下游并在转换模块中使用，各字段以逗号分隔。可用的可读元数据包括：op_ts。
      </td>
    </tr>
    <tr>
     <td>scan.incremental.snapshot.unbounded-chunk-first.enabled</td>
     <td>optional</td>
     <td style="word-wrap: break-word;">false</td>
     <td>String</td>
     <td>
        在快照读取阶段，是否优先分配无界分块。<br>
        这有助于降低在对最大无界分块进行快照时，TaskManager 发生内存溢出（OOM）错误的风险。<br>
        此为实验性选项，默认值为 false。
      </td>
    </tr>
    <tr>
      <td>scan.include-partitioned-tables.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>
        启用 PostgreSQL 分区表的分区路由功能。<br>
        <b>启用时：</b><br>
        (1) 子分区表的事件将被路由到它们的父表。<br>
        (2) 对于 PostgreSQL 11+：配合 PUBLICATION 中的 <code>publish_via_partition_root=true</code> 参数使用可获得更好的性能。<br>
        (3) 对于 PostgreSQL 10：此选项通过从子分区加载模式（其中包含主键）来启用分区路由。<br>
        (4) 使用 <code>partition.tables</code> 指定哪些父表应参与分区路由。<br>
        <b>表列表注意事项：</b> 确保您的表匹配模式捕获您想要的表（PostgreSQL 11+ 使用父表，PostgreSQL 10 使用子表）。
      </td>
    </tr>
    <tr>
      <td>partition.tables</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>
        用于将子分区表事件路由到父表的分区表模式。支持正则表达式。<br>
        点号（.）用于分隔命名空间（数据库）、模式与表名；在正则表达式中需要使用反斜杠对点号进行转义。<br>
        示例：<code>aia_test\.public\.orders_\d{6}</code><br>
        注意：此选项需与 <code>scan.include-partitioned-tables.enabled</code> 一起使用。
      </td>
    </tr>
    </tbody>
</table>
</div>

## 分区路由写法说明

当 <code>scan.include-partitioned-tables.enabled</code> 为 true 时，可通过如下多种写法指定分区路由。右侧“子表模式”中仅表名部分作为正则匹配，命名空间与模式按字面量精确匹配。若模式中包含命名空间（catalog），在匹配时会被忽略（允许写，匹配时不强制要求）。

- 冒号写法（显式 parent:child）
  - 三段：<code>namespace.schema.parent:namespace.schema.child_regex</code>
  - 两段：<code>schema.parent:schema.child_regex</code>
  - 仅表名：<code>parent:child_regex</code>（路由时父表将继承子表的 schema）

- 无冒号写法（仅子表正则）
  - 三段：<code>namespace.schema.child_regex</code>
  - 两段：<code>schema.child_regex</code>
  - 仅表名：<code>child_regex</code>

正则中的点号转义示例：

```text
aia_test\.public\.orders_\d{6}   # 匹配 namespace 相同、schema=public、表名 orders_YYYYMM
public\.orders_\d{6}              # 匹配 schema=public、表名 orders_YYYYMM
orders_\d{6}                      # 匹配任意 schema、表名 orders_YYYYMM（路由时继承子表的 schema）
```

选择器合成规则（最终生效的捕获列表）：

- 最终 include 列表 = “子表正则集合” + “未被子表正则覆盖的剩余父表”。
- 通过冒号左侧显式指定的父表，或根据子表正则推导出的父表（例如将 <code>orders_\d{6}</code> 推导为 <code>orders</code>，会去掉末尾下划线 <code>_</code>），不会重复追加到 include 列表中。
- 若子表正则为“仅表名”（如 <code>orders_\d{6}</code>），则认为覆盖所有 schema 下的同名父表，这些父表将被排除。

示例

- 无冒号：

```text
tables: aia_test.public.orders,aia_test.public.orders_extend,aia_test.public.vouchers,aia_test.public.static_table
partition.tables: aia_test.public.orders_\d{6},aia_test.public.orders_extend_\d{6},aia_test.public.vouchers_\d{6}
```

最终用于匹配的输出：

```text
aia_test.public.orders_\d{6},aia_test.public.orders_extend_\d{6},aia_test.public.vouchers_\d{6},aia_test.public.static_table
```

- 冒号：

```text
tables: aia_test.public.orders,aia_test.public.orders_extend,aia_test.public.vouchers,aia_test.public.static_table
partition.tables: aia_test.public.orders:aia_test.public.orders_\d{6},aia_test.public.orders_extend:aia_test.public.orders_extend_\d{6},aia_test.public.vouchers:aia_test.public.vouchers_\d{6}
```

最终用于匹配的输出：

```text
aia_test.public.orders_\d{6},aia_test.public.orders_extend_\d{6},aia_test.public.vouchers_\d{6},aia_test.public.static_table
```

注意事项

- 右侧子表模式仅表名部分作为正则；schema 必须字面量匹配。
- 若父表仅提供表名，路由后会继承子表的 schema。
- 模式中可包含命名空间（catalog），匹配时会忽略此前缀，建议按团队惯例保持一致以提升可读性。

注意：
1. 配置选项`tables`指定 Postgres CDC 需要采集的表，格式为`db.schema1.tabe1,db.schema2.table2`,其中所有的db需要为同一个db，这是因为postgres链接url中需要指定dbname，目前cdc只支持链接一个db。

## 启动模式

配置选项`scan.startup.mode`指定 Postgres CDC 使用者的启动模式。有效枚举包括：
- `initial` （默认）：在连接器首次启动时，会对被监控的数据库表执行一次初始快照（snapshot），然后继续读取复制插槽中的变更事件。。
- `latest-offset`：在首次启动时不执行快照操作，而是直接从复制插槽的末尾开始读取，即只获取连接器启动之后发生的变更。
- `committed-offset`：跳过快照阶段，从复制插槽中已确认的 confirmed_flush_lsn 偏移量开始读取事件，即从上次提交的位置继续读取变更。
- `snapshot`: 只进行快照阶段，跳过增量阶段，快照阶段读取结束后退出。

### 可用的指标

指标系统能够帮助了解分片分发的进展， 下面列举出了支持的 Flink 指标 [Flink metrics](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/metrics/):

| Group                  | Name                       | Type  | Description    |
|------------------------|----------------------------|-------|----------------|
| namespace.schema.table | isSnapshotting             | Gauge | 表是否在快照读取阶段     |     
| namespace.schema.table | isStreamReading            | Gauge | 表是否在增量读取阶段     |
| namespace.schema.table | numTablesSnapshotted       | Gauge | 已经被快照读取完成的表的数量 |
| namespace.schema.table | numTablesRemaining         | Gauge | 还没有被快照读取的表的数据  |
| namespace.schema.table | numSnapshotSplitsProcessed | Gauge | 正在处理的分片的数量     |
| namespace.schema.table | numSnapshotSplitsRemaining | Gauge | 还没有被处理的分片的数量   |
| namespace.schema.table | numSnapshotSplitsFinished  | Gauge | 已经处理完成的分片的数据   |
| namespace.schema.table | snapshotStartTime          | Gauge | 快照读取阶段开始的时间    |
| namespace.schema.table | snapshotEndTime            | Gauge | 快照读取阶段结束的时间    |

注意:
1. Group 名称是 `namespace.schema.table`，这里的 `namespace` 是实际的数据库名称， `schema` 是实际的 schema 名称， `table` 是实际的表名称。

## 数据类型映射

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">PostgreSQL type<a href="https://www.postgresql.org/docs/12/datatype.html"></a></th>
        <th class="text-left">CDC type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>
        BOOLEAN <br>
        BIT(1) <br>
      <td>BOOLEAN</td>
    </tr>
    <tr>
      <td>
        BIT( > 1)
      <td>BYTES</td>
    </tr>
    <tr>
      <td>
        SMALLINT<br>
        INT2<br>
        SMALLSERIAL<br>
        SERIAL2</td>
      <td>SMALLINT</td>
    </tr>
    <tr>
      <td>
        INTEGER<br>
        SERIAL</td>
      <td>INT</td>
    </tr>
    <tr>
      <td>
        BIGINT<br>
        BIGSERIAL<br>
        OID<br>
      </td>
      <td>BIGINT</td>
    </tr>
    <tr>
      <td>
        REAL<br>
        FLOAT4
      </td>
      <td>FLOAT</td>
    </tr>
   <tr>
      <td>NUMERIC</td>
      <td>DECIMAL(38, 0)</td>
    </tr>
    <tr>
      <td>DOUBLE PRECISION<br>
          FLOAT8
      </td>
      <td>DOUBLE</td>
    </tr>
     <tr>
       <td> CHAR[(M)]<br>
            VARCHAR[(M)]<br>
            CHARACTER[(M)]<br>
            BPCHAR[(M)]<br>
            CHARACTER VARYING[(M)]
       </td>
       <td>STRING</td>
     </tr>
    <tr>
      <td>TIMESTAMPTZ<br>
          TIMESTAMP WITH TIME ZONE</td>
      <td>ZonedTimestampType</td>
    </tr>
    <tr>
      <td>INTERVAL [P]</td>
      <td>BIGINT</td>
    </tr>
    <tr>
      <td>INTERVAL [P]</td>
      <td>STRING(when <code>debezium.interval.handling.mode</code> is set to string)</td>
    </tr>
    <tr>
      <td>BYTEA</td>
      <td>BYTES or STRING (when <code>debezium.binary.handling.mode</code> is set to base64 or base64-url-safe or hex)</td>
    </tr>
    <tr>
      <td>
        JSON<br>
        JSONB<br>
        XML<br>
        UUID<br>
        POINT<br>
        LTREE<br>
        CITEXT<br>
        INET<br>
        INT4RANGE<br>
        INT8RANGE<br>
        NUMRANGE<br>
        TSRANGE<br>
        DATERANGE<br>
        ENUM
      </td>
      <td>STRING</td>
    </tr>
    </tbody>
</table>
</div>

注意：由于Debezium版本不支持多维数组，目前只支持PostgresSQL的单维数组，如:'ARRAY[1,2,3]'，'int[]'。

### Temporal types Mapping
除了包含时区信息的 PostgreSQL 的 TIMESTAMPTZ 数据类型之外，其他时间类型如何映射取决于连接器配置属性 <code>debezium.time.precision.mode</code> 的值。以下各节将描述这些映射关系：
- debezium.time.precision.mode=adaptive
- debezium.time.precision.mode=adaptive_time_microseconds
- debezium.time.precision.mode=connect

注意： 受限当前CDC对时间类型Time的精度为3，<code>debezium.time.precision.mode</code>为adaptive或adaptive_time_microseconds或connect Time类型都转化为Time(3)类型。

<u>debezium.time.precision.mode=adaptive</u>

当<code>debezium.time.precision.mode</code>属性设置为默认的 adaptive（自适应）时，TIME的精度为3，TIMESTAMP的精度为6。
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">PostgreSQL type<a href="https://www.postgresql.org/docs/12/datatype.html"></a></th>
        <th class="text-left">CDC type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
       <tr>
        <td>
          DATE
        <td>DATE</td>
      </tr>
      <tr>
        <td>
          TIME([P])
        </td>
        <td>TIME(3)</td>
      </tr>
      <tr>
        <td>
          TIMESTAMP([P])
        </td>
        <td>TIMESTAMP([P])</td>
      </tr>
    </tbody>
</table>
</div>

<u>debezium.time.precision.mode=adaptive_time_microseconds</u>

当<code>debezium.time.precision.mode</code>属性设置为默认的 adaptive_time_microseconds时，TIME的精度为3，TIMESTAMP的精度为6。
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">PostgreSQL type<a href="https://www.postgresql.org/docs/12/datatype.html"></a></th>
        <th class="text-left">CDC type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
       <tr>
        <td>
          DATE
        <td>DATE</td>
      </tr>
      <tr>
        <td>
          TIME([P])
        </td>
        <td>TIME(3)</td>
      </tr>
      <tr>
        <td>
          TIMESTAMP([P])
        </td>
        <td>TIMESTAMP([P])</td>
      </tr>
    </tbody>
</table>
</div>

<u>debezium.time.precision.mode=connect</u>

当<code>debezium.time.precision.mode</code>属性设置为默认的 connect时，TIME和TIMESTAMP的精度都为3。
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">PostgreSQL type<a href="https://www.postgresql.org/docs/12/datatype.html"></a></th>
        <th class="text-left">CDC type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
       <tr>
        <td>
          DATE
        <td>DATE</td>
      </tr>
      <tr>
        <td>
          TIME([P])
        </td>
        <td>TIME(3)</td>
      </tr>
      <tr>
        <td>
          TIMESTAMP([P])
        </td>
        <td>TIMESTAMP(3)</td>
      </tr>
    </tbody>
</table>
</div>

### Decimal types Mapping
PostgreSQL 连接器配置属性 <code>debezium.decimal.handling.mode</code> 的设置决定了连接器如何映射十进制类型。

当 <code>debezium.decimal.handling.mode</code> 属性设置为 precise（精确）时，连接器会对所有 DECIMAL、NUMERIC 和 MONEY 列使用 Kafka Connect 的 org.apache.kafka.connect.data.Decimal 逻辑类型。这是默认模式。
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">PostgreSQL type<a href="https://www.postgresql.org/docs/12/datatype.html"></a></th>
        <th class="text-left">CDC type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
       <tr>
        <td>
          NUMERIC[(M[,D])]
        <td>DECIMAL[(M[,D])]</td>
      </tr>
      <tr>
        <td>
          NUMERIC
        <td>DECIMAL(38,0)</td>
      </tr>
      <tr>
        <td>
          DECIMAL[(M[,D])]
        <td>DECIMAL[(M[,D])]</td>
      </tr>
      <tr>
        <td>
          DECIMAL
        <td>DECIMAL(38,0)</td>
      </tr>
      <tr>
        <td>
          MONEY[(M[,D])]
        <td>DECIMAL(38,digits)(schema 参数 scale 包含一个整数，表示小数点移动了多少位。scale schema 参数由 money.fraction.digits 连接器配置属性决定。)</td>
      </tr>
    </tbody>
</table>
</div>

当 <code>debezium.decimal.handling.mode</code> 属性设置为 double 时，连接器将所有 DECIMAL、NUMERIC 和 MONEY 值表示为 Java 的 double 值，并按照下表所示进行编码。

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">PostgreSQL type<a href="https://www.postgresql.org/docs/12/datatype.html"></a></th>
        <th class="text-left">CDC type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
       <tr>
        <td>
          NUMERIC[(M[,D])]
        <td>DOUBLE</td>
      </tr>
      <tr>
        <td>
          DECIMAL[(M[,D])]
        <td>DOUBLE</td>
      </tr>
      <tr>
        <td>
          MONEY[(M[,D])]
        <td>DOUBLE</td>
      </tr>
    </tbody>
</table>
</div>

<code>debezium.decimal.handling.mode</code> 配置属性的最后一个可选设置是 string（字符串）。在这种情况下，连接器将 DECIMAL、NUMERIC 和 MONEY 值表示为其格式化的字符串形式，并按照下表所示进行编码。
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">PostgreSQL type<a href="https://www.postgresql.org/docs/12/datatype.html"></a></th>
        <th class="text-left">CDC type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
       <tr>
        <td>
          NUMERIC[(M[,D])]
        <td>STRING</td>
      </tr>
      <tr>
        <td>
          DECIMAL[(M[,D])]
        <td>STRING</td>
      </tr>
      <tr>
        <td>
          MONEY[(M[,D])]
        <td>STRING</td>
      </tr>
    </tbody>
</table>
</div>

当 <code>debezium.decimal.handling.mode</code> 的设置为 string 或 double 时，PostgreSQL 支持将 NaN（非数字）作为一个特殊值存储在 DECIMAL/NUMERIC 值中。在这种情况下，连接器会将 NaN 编码为 Double.NaN 或字符串常量 NAN。

### HSTORE type Mapping
PostgreSQL 连接器配置属性 <code>debezium.hstore.handling.mode</code> 的设置决定了连接器如何映射 HSTORE 值。

当 <code>debezium.hstore.handling.mode</code> 属性设置为 json（默认值）时，连接器将 HSTORE 值表示为 JSON 值的字符串形式，并按照下表所示进行编码。当 <code>debezium.hstore.handling.mode</code> 属性设置为 map 时，连接器对 HSTORE 值使用 MAP 模式类型。
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">PostgreSQL type<a href="https://www.postgresql.org/docs/12/datatype.html"></a></th>
        <th class="text-left">CDC type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
       <tr>
        <td>
         HSTORE
        <td>STRING(<code>`debezium.hstore.handling.mode`=`string`</code>)</td>
      </tr>
      <tr>
        <td>
         HSTORE
        <td>MAP(<code>`debezium.hstore.handling.mode`=`map`</code>)</td>
      </tr>
    </tbody>
</table>
</div>

### Network address types Mapping
PostgreSQL 拥有可以存储 IPv4、IPv6 和 MAC 地址的数据类型。使用这些类型来存储网络地址比使用纯文本类型更为合适。网络地址类型提供了输入错误检查以及专用的操作符和函数。
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">PostgreSQL type<a href="https://www.postgresql.org/docs/12/datatype.html"></a></th>
        <th class="text-left">CDC type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
       <tr>
        <td>
         INET
        <td>STRING</td>
      </tr>
      <tr>
        <td>
         CIDR
        <td>STRING</td>
      </tr>
      <tr>
        <td>
         MACADDR
        <td>STRING</td>
      </tr>
      <tr>
        <td>
         MACADDR8
        <td>STRING</td>
      </tr> 
    </tbody>
</table>
</div>

### PostGIS Types Mapping
PostgreSQL 通过 PostGIS 扩展支持空间数据类型：
```
    GEOMETRY(POINT, xx): 在笛卡尔坐标系中表示一个点，其中 EPSG:xx 定义了坐标系。它适用于局部平面计算。
    GEOGRAPHY(MULTILINESTRING): 在基于球面模型的纬度和经度上存储多条线串。它适用于全球范围的空间分析。
```
前者适用于小范围的平面数据，而后者适用于需要考虑地球曲率的大范围数据。
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">Spatial data in Postgres</th>
        <th class="text-left">Json String converted in Flink</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td>GEOMETRY(POINT, xx)</td>
        <td>{"coordinates":"[[174.9479, -36.7208]]","type":"Point","srid":3187}"</td>
      </tr>
      <tr>
        <td>GEOGRAPHY(MULTILINESTRING)</td>
        <td>{"coordinates":"[[169.1321, -44.7032],[167.8974, -44.6414]]","type":"MultiLineString","srid":4326}</td>
      </tr>
    </tbody>
</table>
</div>

{{< top >}}
