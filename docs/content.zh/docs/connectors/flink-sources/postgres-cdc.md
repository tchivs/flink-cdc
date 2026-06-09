---
title: "Postgres"
weight: 5
type: docs
aliases:
- /connectors/flink-sources/postgres-cdc
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

# Postgres CDC 连接器

Postgres CDC 连接器允许从 PostgreSQL 数据库读取快照数据和增量数据。本文描述了如何设置 Postgres CDC 连接器，以便对 PostgreSQL 数据库运行 SQL 查询。

依赖
------------

为了设置 Postgres CDC 连接器，下表提供了使用构建自动化工具（如 Maven 或 SBT）的项目以及使用 SQL JAR 包的 SQL Client 所需的依赖信息。

### Maven 依赖

{{< artifact flink-connector-postgres-cdc >}}

### SQL Client JAR

```下载链接仅对稳定发布版本可用。```

下载 [flink-sql-connector-postgres-cdc](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-postgres-cdc) 并将其放到 `<FLINK_HOME>/lib/` 目录下。

**注意:** 参考 [flink-sql-connector-postgres-cdc](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-postgres-cdc)，更多已发布版本可以在 Maven 中央仓库获取。

如何创建 Postgres CDC 表
----------------

Postgres CDC 表可以定义如下：

```sql
-- 在 Flink SQL 中注册 PostgreSQL 表 'shipments'
CREATE TABLE shipments (
  shipment_id INT,
  order_id INT,
  origin STRING,
  destination STRING,
  is_arrived BOOLEAN
) WITH (
  'connector' = 'postgres-cdc',
  'hostname' = 'localhost',
  'port' = '5432',
  'username' = 'postgres',
  'password' = 'postgres',
  'database-name' = 'postgres',
  'schema-name' = 'public',
  'table-name' = 'shipments',
  'slot.name' = 'flink',
   -- 实验性功能：增量快照（默认关闭）
  'scan.incremental.snapshot.enabled' = 'true'
);

-- 从 shipments 表读取快照和日志数据
SELECT * FROM shipments;
```

连接器选项
----------------

<div class="highlight">
<table class="colwidths-auto docutils">
   <thead>
      <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-left" style="width: 8%">Required</th>
        <th class="text-left" style="width: 7%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 50%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>connector</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>指定要使用的连接器，此处应为 <code>'postgres-cdc'</code>。</td>
    </tr>
    <tr>
      <td>hostname</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>PostgreSQL 数据库服务器的 IP 地址或主机名。</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接 PostgreSQL 数据库服务器时使用的用户名。</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接 PostgreSQL 数据库服务器时使用的密码。</td>
    </tr>
    <tr>
      <td>database-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>需要监控的 PostgreSQL 服务器数据库名称。</td>
    </tr>
    <tr>
      <td>schema-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>需要监控的 PostgreSQL 数据库 schema 名称。schema 名称支持正则表达式，以监控多个满足正则表达式的 schema。</td>
    </tr>
    <tr>
      <td>table-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>需要监控的 PostgreSQL 数据库表名称。表名支持正则表达式，以监控多个满足正则表达式的表。</td>
    </tr>
    <tr>
      <td>port</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">5432</td>
      <td>Integer</td>
      <td>PostgreSQL 数据库服务器的整数端口号。</td>
    </tr>
    <tr>
      <td>slot.name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>PostgreSQL 逻辑解码 slot 名称，该 slot 由特定插件为特定 database/schema 创建，用于流式传输变更。服务器会使用该 slot 向当前配置的连接器发送事件。
          <br/>Slot 名称必须符合 <a href="https://www.postgresql.org/docs/current/static/warm-standby.html#STREAMING-REPLICATION-SLOTS-MANIPULATION">PostgreSQL replication slot 命名规则</a>，即每个 replication slot 都有一个名称，名称可以包含小写字母、数字和下划线。</td>
    </tr> 
    <tr>
      <td>decoding.plugin.name</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">decoderbufs</td>
      <td>String</td>
      <td>服务器上安装的 Postgres 逻辑解码插件名称。支持的取值为 decoderbufs、wal2json、wal2json_rds、wal2json_streaming、wal2json_rds_streaming 和 pgoutput。</td>
    </tr>
    <tr>
      <td>changelog-mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">all</td>
      <td>String</td>
      <td>用于编码流式变更的 changelog 模式。支持的取值为 <code>all</code>（使用所有 RowKind 将变更编码为 retract stream）和 <code>upsert</code>（将变更编码为 upsert stream，用于描述 key 上的幂等更新）。
          <br/>当无法使用 replica identity <code>FULL</code> 时，带主键的表可以使用 <code>upsert</code> 模式。使用 <code>upsert</code> 模式时必须设置主键。</td>
    </tr>
    <tr>
      <td>heartbeat.interval.ms</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">30s</td>
      <td>Duration</td>
      <td>发送心跳事件的间隔，用于跟踪最新可用的 replication slot offset。</td>
    </tr>
   <tr>
      <td>debezium.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>将 Debezium 属性透传给 Debezium Embedded Engine，用于从 Postgres 服务器捕获数据变更。
          例如：<code>'debezium.snapshot.mode' = 'never'</code>。
          更多信息请参见 <a href="https://debezium.io/documentation/reference/1.9/connectors/postgresql.html#postgresql-connector-properties">Debezium Postgres 连接器属性</a>。</td>
    </tr>
    <tr>
      <td>debezium.snapshot.select.statement.overrides</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>如果表中数据量较大且不需要全部历史数据，可以通过 Debezium 底层配置指定需要快照读取的数据范围。该参数只影响快照，不影响后续数据读取消费。
        <br/> 注意：PostgreSQL 必须使用 schema 名称和表名称。
        <br/> 例如：<code>'debezium.snapshot.select.statement.overrides' = 'schema.table'</code>。
        <br/> 指定上述属性后，还必须添加以下属性：
        <code> debezium.snapshot.select.statement.overrides.[schema].[table] </code>
      </td>
    </tr>
    <tr>
      <td>debezium.snapshot.select.statement.overrides.[schema].[table]</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>可以指定 SQL 语句来限制快照读取的数据范围。
        <br/> 注意 1：SQL 语句中需要指定 schema 和表，SQL 应符合数据源的语法。
        <br/> 例如：<code>'debezium.snapshot.select.statement.overrides.schema.table' = 'select * from schema.table where 1 != 1'</code>。
        <br/> 注意 2：Flink SQL Client 提交任务时，不支持内容中包含单引号的函数。
        <br/> 例如：<code>'debezium.snapshot.select.statement.overrides.schema.table' = 'select * from schema.table where to_char(rq, 'yyyy-MM-dd')'</code>。
      </td>
    </tr>
    <tr>
          <td>scan.incremental.snapshot.enabled</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">false</td>
          <td>Boolean</td>
          <td>增量快照是一种新的表快照读取机制。相比旧的快照机制，增量快照有以下优势：
                (1) source 可以在快照读取阶段并行执行；
                (2) source 可以在快照读取阶段以 chunk 粒度执行 checkpoint；
                (3) source 在快照读取前不需要获取全局读锁（FLUSH TABLES WITH READ LOCK）。
              更多信息请参见 <a href="#incremental-snapshot-reading">增量快照读取</a>。
          </td>
    </tr>
    <tr>
      <td>scan.incremental.close-idle-reader.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否在快照阶段结束后关闭空闲 reader。<br>
          当 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' 设置为 true 时，需要 Flink 版本大于等于 1.14。<br>
          如果 Flink 版本大于等于 1.15，'execution.checkpointing.checkpoints-after-tasks-finish.enabled' 的默认值已变为 true，因此无需显式配置 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' = 'true'。
      </td>
    </tr>
    <tr>
      <td>scan.lsn-commit.checkpoints-num-delay</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>开始提交 LSN 偏移前需要延迟的 checkpoint 数量。<br>
          checkpoint LSN 偏移会以滚动方式提交，延迟 checkpoint 中最早的 checkpoint 标识会最先提交。
      </td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.unbounded-chunk-first.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>
        在快照读取阶段是否优先分配无界 chunk。<br>
        这有助于降低对最大无界 chunk 执行快照时 TaskManager 发生内存溢出（OOM）的风险。<br>
      </td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.backfill.skip</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>
        是否在快照读取阶段跳过 backfill。<br>
        如果跳过 backfill，快照阶段捕获表上的变更会在之后的变更日志读取阶段被消费，而不是合并到快照中。<br>
        警告：跳过 backfill 可能导致数据不一致，因为快照阶段发生的某些变更日志事件可能会被重放（仅保证 at-least-once 语义）。
        例如，对快照中已经更新过的值再次更新，或删除快照中已经删除的数据。这些被重放的变更日志事件需要特殊处理。
      </td>
    </tr>
    <tr>
      <td>scan.read-changelog-as-append-only.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>
        是否将 changelog 数据流转换为 append-only 数据流。<br>
        仅在需要保存上游表删除消息等特殊场景下开启使用，比如在逻辑删除场景下，用户不允许物理删除下游消息，此时使用该特性，并配合 row_kind 元数据字段，下游可以先保存所有明细数据，再通过 row_kind 字段判断是否进行逻辑删除。<br>
        参数取值如下：<br>
          <li>true：所有类型的消息（包括INSERT、DELETE、UPDATE_BEFORE、UPDATE_AFTER）都会转换成 INSERT 类型的消息。</li>
          <li>false（默认）：所有类型的消息都保持原样下发。</li>
      </td>
    </tr>
    <tr>
      <td>scan.include-partitioned-tables.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>
        是否发现被捕获分区表的物理子分区，使用子分区进行快照切分和日志捕获，并将输出记录路由到分区根表。<br>
        开启后，表列表中只能配置分区父表或子分区表其中一种，不能同时配置父表和子分区表。
      </td>
    </tr>
    <tr>
      <td>scan.partition.publication.refresh.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>
        是否自动把发现的子分区表添加到配置的 PostgreSQL publication 中。<br>
        该选项仅在 <code>scan.include-partitioned-tables.enabled</code> 为 true、<code>decoding.plugin.name</code> 为 <code>pgoutput</code>、<code>scan.startup.mode</code> 不是 <code>snapshot</code>，且 <code>debezium.publication.autocreate.mode</code> 不是 <code>all_tables</code> 时有效。<br>
        当 <code>debezium.publication.autocreate.mode</code> 为 <code>disabled</code> 时，连接器会把缺失的子分区表添加到已有 publication 中，且连接器用户必须具备修改 publication 的权限。若在 <code>disabled</code> 模式下未开启该选项，子分区表必须已包含在 publication 中。<br>
        当 <code>debezium.publication.autocreate.mode</code> 为 <code>filtered</code> 时，Debezium 会根据连接器表过滤条件创建或更新 publication 成员，此时不需要连接器侧刷新 publication。
      </td>
    </tr>
    </tbody>
    </table>
</div>
<div>

### 注意事项

#### `slot.name` 选项

建议为不同的表设置不同的 `slot.name`，以避免潜在的 `PSQLException: ERROR: replication slot "flink" is active for PID 974` 错误。更多信息请参见[这里](https://debezium.io/documentation/reference/1.9/connectors/postgresql.html#postgresql-property-slot-name)。

#### `scan.lsn-commit.checkpoints-num-delay` 选项

消费 PostgreSQL 日志时，必须提交 LSN offset 才能触发对应 slot 的日志数据清理。但是，一旦 LSN offset 被提交，更早的 offset 将失效。为了确保作业恢复时仍可访问较早的 LSN offset，连接器会将 LSN 提交延迟 `scan.lsn-commit.checkpoints-num-delay` 个 checkpoint（默认值为 `3`）。该功能仅在配置项 `scan.incremental.snapshot.enabled` 设置为 true 时可用。

### 增量快照选项

以下选项仅在 `scan.incremental.snapshot.enabled=true` 时可用：

<div class="highlight">
<table class="colwidths-auto docutils">
   <thead>
      <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-left" style="width: 8%">Required</th>
        <th class="text-left" style="width: 7%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 50%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
          <td>scan.incremental.snapshot.chunk.size</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">8096</td>
          <td>Integer</td>
          <td>表快照的 chunk 大小（行数），读取表快照时捕获表会被拆分为多个 chunk。</td>
    </tr>
    <tr>
      <td>scan.startup.mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">initial</td>
      <td>String</td>
      <td>Postgres CDC 消费者可选的启动模式，合法值为 "initial"、"latest-offset"、"committed-offset" 和 "snapshot"。
           更多信息请参见<a href="#启动模式">启动模式</a>。</td>
    </tr>
    <tr>
      <td>chunk-meta.group.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Integer</td>
      <td>chunk 元数据的分组大小。如果元数据大小超过该分组大小，元数据会被拆分为多个分组。</td>
    </tr>
    <tr>
          <td>connect.timeout</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">30s</td>
          <td>Duration</td>
          <td>连接器尝试连接 PostgreSQL 数据库服务器后，在超时前等待的最长时间。</td>
    </tr>
    <tr>
          <td>connect.pool.size</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">30</td>
          <td>Integer</td>
          <td>连接池大小。</td>
    </tr>
    <tr>
          <td>connect.max-retries</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">3</td>
          <td>Integer</td>
          <td>连接器建立数据库服务器连接时的最大重试次数。</td>
    </tr>
    <tr>
          <td>scan.snapshot.fetch.size</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">1024</td>
          <td>Integer</td>
          <td>读取表快照时每次 poll 的最大拉取条数。</td>
    </tr>
    <tr>
          <td>scan.incremental.snapshot.chunk.key-column</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">(none)</td>
          <td>String</td>
          <td>表快照的 chunk key。读取表快照时，被捕获的表会按照 chunk key 拆分为多个 chunk。
            默认情况下，chunk key 是主键的第一列。非主键列也可以作为 chunk key，但这可能导致查询性能下降。
            <br>
            <b>警告：</b> 使用非主键列作为 chunk key 可能导致数据不一致。详情请参见<a href="#警告">警告</a>。
          </td>
    </tr>
    <tr>
          <td>chunk-key.even-distribution.factor.lower-bound</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">0.05d</td>
          <td>Double</td>
          <td>chunk key 分布因子的下界。分布因子用于判断表数据是否均匀分布。
              当数据分布均匀时，表 chunk 会使用均匀计算优化；当数据分布不均匀时，会执行拆分查询。
              分布因子的计算方式为 (MAX(id) - MIN(id) + 1) / rowCount。</td>
    </tr>
    <tr>
          <td>chunk-key.even-distribution.factor.upper-bound</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">1000.0d</td>
          <td>Double</td>
          <td>chunk key 分布因子的上界。分布因子用于判断表数据是否均匀分布。
              当数据分布均匀时，表 chunk 会使用均匀计算优化；当数据分布不均匀时，会执行拆分查询。
              分布因子的计算方式为 (MAX(id) - MIN(id) + 1) / rowCount。</td>
    </tr>
    </tbody>
</table>
</div>

支持的元数据
----------------

下表中的元数据可以在表定义中作为只读（VIRTUAL）列声明。

<table class="colwidths-auto docutils">
  <thead>
     <tr>
       <th class="text-left" style="width: 15%">Key</th>
       <th class="text-left" style="width: 30%">DataType</th>
       <th class="text-left" style="width: 55%">Description</th>
     </tr>
  </thead>
  <tbody>
    <tr>
      <td>table_name</td>
      <td>STRING NOT NULL</td>
      <td>当前记录所属的表名称。</td>
    </tr>
    <tr>
      <td>schema_name</td>
      <td>STRING NOT NULL</td>
      <td>当前记录所属的 schema 名称。</td>
    </tr>
    <tr>
      <td>database_name</td>
      <td>STRING NOT NULL</td>
      <td>当前记录所属的数据库名称。</td>
    </tr>
    <tr>
      <td>op_ts</td>
      <td>TIMESTAMP_LTZ(3) NOT NULL</td>
      <td>表示该变更在数据库中发生的时间。<br>如果记录是从表快照而不是变更流中读取的，则该值始终为 0。</td>
    </tr>
  </tbody>
</table>

限制
--------

### 禁用增量快照时，扫描表快照期间无法执行 checkpoint

当 `scan.incremental.snapshot.enabled=false` 时，存在以下限制。

扫描数据库表快照期间，由于没有可恢复的位置，无法执行 checkpoint。为了避免执行 checkpoint，Postgres CDC Source 会让 checkpoint 一直等待直到超时。超时的 checkpoint 会被识别为失败的 checkpoint，默认情况下会触发 Flink 作业 failover。因此，如果数据库表较大，建议添加以下 Flink 配置，以避免 checkpoint 超时导致 failover：

```
execution.checkpointing.interval: 10min
execution.checkpointing.tolerable-failed-checkpoints: 100
restart-strategy: fixed-delay
restart-strategy.fixed-delay.attempts: 2147483647
```

以下扩展的 CREATE TABLE 示例展示了如何暴露这些元数据字段：
```sql
CREATE TABLE products (
    db_name STRING METADATA FROM 'database_name' VIRTUAL,
    table_name STRING METADATA  FROM 'table_name' VIRTUAL,
    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
    shipment_id INT,
    order_id INT,
    origin STRING,
    destination STRING,
    is_arrived BOOLEAN
) WITH (
  'connector' = 'postgres-cdc',
  'hostname' = 'localhost',
  'port' = '5432',
  'username' = 'postgres',
  'password' = 'postgres',
  'database-name' = 'postgres',
  'schema-name' = 'public',
  'table-name' = 'shipments',
  'slot.name' = 'flink'
);
```

支持的特性
--------

### 增量快照读取（实验性）<a name="incremental-snapshot-reading" id="incremental-snapshot-reading"></a>

增量快照读取是一种读取表快照的新机制。与旧的快照机制相比，增量快照具有许多优势，包括：
* （1）PostgreSQL CDC Source 在快照读取期间可以并行执行
* （2）PostgreSQL CDC Source 在快照读取期间可以按 chunk 粒度执行 checkpoint
* （3）PostgreSQL CDC Source 在快照读取前不需要获取全局读锁

在增量快照读取过程中，PostgreSQL CDC Source 首先根据用户指定的表 chunk key 拆分快照 chunk（split），然后将这些 chunk 分配给多个 reader 来读取快照 chunk 数据。

### Exactly-Once 处理

Postgres CDC 连接器是一个 Flink Source 连接器，会先读取数据库快照，然后继续读取日志；即使发生故障，也能提供 **exactly-once processing**。请阅读[连接器工作原理](https://debezium.io/documentation/reference/1.9/connectors/postgresql.html#how-the-postgresql-connector-works)了解更多信息。

### 启动模式

配置项 `scan.startup.mode` 指定 PostgreSQL CDC 消费者的启动模式。有效枚举值如下：

- `initial`（默认）：首次启动时对被监控的数据库表执行初始快照，然后继续读取 replication slot。
- `latest-offset`：首次启动时不对被监控的数据库表执行快照，只从 replication 的末尾开始读取，也就是只读取连接器启动后的变更。
- `committed-offset`：跳过快照阶段，从 replication slot 的 `confirmed_flush_lsn` offset 开始读取事件。
- `snapshot`：只执行快照阶段，快照阶段读取完成后退出。

### 动态加表

**注意:** 该功能从 Flink CDC 3.1.0 版本开始支持。

动态加表功能使你可以为正在运行的作业添加新表进行监控。新添加的表将首先读取其快照数据,然后自动读取其 WAL (Write-Ahead Log) 日志 或者 replication slot changes 复制槽。

想象一下这个场景:一开始,Flink 作业监控表 `[product, user, address]`,但几天后,我们希望这个作业还可以监控表 `[order, custom]`,这些表包含历史数据,我们需要作业仍然可以复用作业的已有状态。动态加表功能可以优雅地解决此问题。

以下操作显示了如何启用此功能来解决上述场景。使用现有的 PostgreSQL CDC Source 作业,如下:

```java
    JdbcIncrementalSource<String> postgresSource =
            PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                .hostname("yourHostname")
                .port(5432)
                .database("postgres") // 设置捕获的数据库
                .schemaList("inventory") // 设置捕获的 schema
                .tableList("inventory.product", "inventory.user", "inventory.address") // 设置捕获的表
                .username("yourUsername")
                .password("yourPassword")
                .slotName("flink")
                .scanNewlyAddedTableEnabled(true) // 启用扫描新添加的表功能
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();
   // 你的业务代码
```

如果我们想添加新表 `[inventory.order, inventory.custom]` 到现有的 Flink 作业,只需更新作业的 `tableList()` 将新增表 `[inventory.order, inventory.custom]` 加入并从已有的 savepoint 恢复作业。

_步骤 1_: 使用 savepoint 停止现有的 Flink 作业。
```shell
$ ./bin/flink stop $Existing_Flink_JOB_ID
```
```shell
Suspending job "cca7bc1061d61cf15238e92312c2fc20" with a savepoint.
Savepoint completed. Path: file:/tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab
```
_步骤 2_: 更新现有 Flink 作业的表列表选项。
1. 更新 `tableList()` 参数。
2. 编译更新后的作业,示例如下:
```java
    JdbcIncrementalSource<String> postgresSource =
            PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                .hostname("yourHostname")
                .port(5432)
                .database("postgres")
                .schemaList("inventory")
                .tableList("inventory.product", "inventory.user", "inventory.address", "inventory.order", "inventory.custom") // 设置捕获的表 [product, user, address, order, custom]
                .username("yourUsername")
                .password("yourPassword")
                .slotName("flink")
                .scanNewlyAddedTableEnabled(true)
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();
   // 你的业务代码
```
_步骤 3_: 从 savepoint 还原更新后的 Flink 作业。
```shell
$ ./bin/flink run \
      --detached \
      --from-savepoint /tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab \
      ./FlinkCDCExample.jar
```
**注意:** 请参考文档[从之前的 savepoint 恢复作业](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/deployment/cli/#command-line-interface)了解更多详细信息。

### DataStream Source 数据源

Postgres CDC 连接器也可以作为 DataStream Source。DataStream Source 有两种模式：

- 基于增量快照的模式，支持并行读取
- 基于 SourceFunction 的模式，仅支持单线程读取

#### 基于增量快照的 DataStream（实验性）

```java
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PostgresParallelSourceExample {

    public static void main(String[] args) throws Exception {

        DebeziumDeserializationSchema<String> deserializer =
                new JsonDebeziumDeserializationSchema();

        JdbcIncrementalSource<String> postgresIncrementalSource =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .hostname("localhost")
                        .port(5432)
                        .database("postgres")
                        .schemaList("inventory")
                        .tableList("inventory.products")
                        .username("postgres")
                        .password("postgres")
                        .slotName("flink")
                        .decodingPluginName("decoderbufs") // PostgreSQL 10+ 请使用 pgoutput
                        .deserializer(deserializer)
                        .splitSize(2) // 每个快照 split 的大小
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(3000);

        env.fromSource(
                        postgresIncrementalSource,
                        WatermarkStrategy.noWatermarks(),
                        "PostgresParallelSource")
                .setParallelism(2)
                .print();

        env.execute("Output Postgres Snapshot");
    }
}
```

#### 基于 SourceFunction 的 DataStream

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.cdc.connectors.postgres.PostgreSQLSource;

public class PostgreSQLSourceExample {
  public static void main(String[] args) throws Exception {
    SourceFunction<String> sourceFunction = PostgreSQLSource.<String>builder()
      .hostname("localhost")
      .port(5432)
      .database("postgres") // 监控 postgres 数据库
      .schemaList("inventory")  // 监控 inventory schema
      .tableList("inventory.products") // 监控 products 表
      .username("flinkuser")
      .password("flinkpw")
      .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
      .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env
      .addSource(sourceFunction)
      .print().setParallelism(1); // sink 使用并行度 1 以保持消息顺序

    env.execute();
  }
}
```

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
2. 对于 PostgreSQL，Group 的名称会类似于 `test_database.test_schema.test_table`。

### 无主键表

从 3.4.0 版本开始，Postgres CDC 支持没有主键的表。使用无主键表时，必须配置 `scan.incremental.snapshot.chunk.key-column` 选项并指定一个非空字段。

需要注意以下两点。

1. 如果表中存在索引，请尽量在 `scan.incremental.snapshot.chunk.key-column` 中使用索引包含的列，这可以提升 select 语句的执行速度。
2. 无主键 Postgres CDC 表的处理语义取决于 `scan.incremental.snapshot.chunk.key-column` 指定列的行为。
* 如果指定列上没有发生 update 操作，则可以保证 exactly-once 语义。
* 如果指定列上发生 update 操作，则只能保证 at-least-once 语义。不过，可以在下游指定主键并执行幂等操作来保证数据正确性。

#### 警告

对于带主键的 Postgres 表，如果使用**非主键列**作为 `scan.incremental.snapshot.chunk.key-column`，可能导致数据不一致。下面的场景说明了这个问题以及降低潜在问题的建议。

#### 问题场景

- **表结构:**
    - **主键:** `id`
    - **Chunk Key 列:** `pid`（不是主键）

- **快照 Split:**
    - **Split 0:** `1 < pid <= 3`
    - **Split 1:** `3 < pid <= 5`

- **操作:**
    - 两个不同的 subtask 正在并发读取 Split 0 和 Split 1。
    - 两个 split 正在读取期间，某个 update 操作将 `id=0` 的 `pid` 从 `2` 改为 `4`。该 update 发生在两个 split 的低水位线和高水位线之间。

- **结果:**
    - **Split 0:** 包含记录 `[id=0, pid=2]`
    - **Split 1:** 包含记录 `[id=0, pid=4]`

由于这些记录的处理顺序无法保证，`id=0` 的 `pid` 最终值可能是 `2`，也可能是 `4`，从而导致潜在的数据不一致。

数据类型映射
----------------

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">PostgreSQL type<a href="https://www.postgresql.org/docs/12/datatype.html"></a></th>
        <th class="text-left">Flink SQL type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td></td>
      <td>TINYINT</td>
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
        BIGSERIAL</td>
      <td>BIGINT</td>
    </tr>
   <tr>
      <td></td>
      <td>DECIMAL(20, 0)</td>
    </tr>
    <tr>
      <td>BIGINT</td>
      <td>BIGINT</td>
    </tr>
    <tr>
      <td>
        REAL<br>
        FLOAT4</td>
      <td>FLOAT</td>
    </tr>
    <tr>
      <td>
        FLOAT8<br>
        DOUBLE PRECISION</td>
      <td>DOUBLE</td>
    </tr>
    <tr>
      <td>
        NUMERIC(p, s)<br>
        DECIMAL(p, s)</td>
      <td>DECIMAL(p, s)</td>
    </tr>
    <tr>
      <td>BOOLEAN</td>
      <td>BOOLEAN</td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>DATE</td>
    </tr>
    <tr>
      <td>TIME [(p)] [WITHOUT TIMEZONE]</td>
      <td>TIME [(p)] [WITHOUT TIMEZONE]</td>
    </tr>
    <tr>
      <td>TIMESTAMP [(p)] [WITHOUT TIMEZONE]</td>
      <td>TIMESTAMP [(p)] [WITHOUT TIMEZONE]</td>
    </tr>
    <tr>
      <td>
        CHAR(n)<br>
        CHARACTER(n)<br>
        VARCHAR(n)<br>
        CHARACTER VARYING(n)<br>
        UUID<br>
        TEXT</td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>BYTEA</td>
      <td>BYTES</td>
    </tr>
    </tbody>
</table>
</div>

{{< top >}}
