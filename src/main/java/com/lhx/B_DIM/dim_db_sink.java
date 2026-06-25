package com.lhx.B_DIM;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * DIM层配置化维表分流写HBase。
 *
 * @author 刘海旭
 * @date 2026/05/26
 */
public class dim_db_sink {

    /** Kafka 集群地址。 */
    private static final String KAFKA_BOOTSTRAP_SERVERS = "node101:9092,node102:9092,node103:9092";

    /** ODS层业务数据主题。 */
    private static final String SOURCE_TOPIC_ODS_DB = "ods_db";

    /** 当前作业的 Kafka 消费者组。 */
    private static final String CONSUMER_GROUP_ID = "dim_db_sink";

    /** MySQL 配置库连接信息。 */
    private static final String MYSQL_HOSTNAME = "node101";
    private static final int MYSQL_PORT = 3306;
    private static final String MYSQL_CONFIG_DATABASE = "realtime_config";
    private static final String MYSQL_CONFIG_TABLE = "realtime_config.table_process_dim";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "123456";
    private static final String MYSQL_SERVER_TIME_ZONE = "Asia/Shanghai";

    /** HBase维表命名空间。 */
    private static final String HBASE_NAMESPACE = "gmall_dim";

    /** 维表配置广播状态描述符，key为来源业务表名。 */
    private static final MapStateDescriptor<String, TableProcessDim> DIM_CONFIG_DESCRIPTOR =
            new MapStateDescriptor<String, TableProcessDim>(
                    "dim-config",
                    String.class,
                    TableProcessDim.class
            );

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Checkpoint 用于保存 Kafka 位点和配置广播状态，失败恢复后继续处理。
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        env.setStateBackend(new HashMapStateBackend());

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(60000L);
        checkpointConfig.setMinPauseBetweenCheckpoints(3000L);
        checkpointConfig.setTolerableCheckpointFailureNumber(3);
        checkpointConfig.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        Properties kafkaConsumerProperties = new Properties();
        kafkaConsumerProperties.setProperty("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
        kafkaConsumerProperties.setProperty("group.id", CONSUMER_GROUP_ID);
        kafkaConsumerProperties.setProperty("auto.offset.reset", "earliest");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(
                SOURCE_TOPIC_ODS_DB,
                new SimpleStringSchema(StandardCharsets.UTF_8),
                kafkaConsumerProperties
        );
        kafkaConsumer.setStartFromGroupOffsets();

        // 主流读取ODS层业务变更数据，非维表数据在DIM层忽略，后续交给DWD层处理。
        DataStreamSource<String> odsDbStream = env.addSource(kafkaConsumer, "kafka-ods-db-source");

        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("database.serverTimezone", MYSQL_SERVER_TIME_ZONE);

        // 配置流监听 MySQL 配置表，配置变更可以动态影响后续维表分流。
        DataStreamSource<String> configStream = env.addSource(
                MySqlSource.<String>builder()
                        .hostname(MYSQL_HOSTNAME)
                        .port(MYSQL_PORT)
                        .databaseList(MYSQL_CONFIG_DATABASE)
                        .tableList(MYSQL_CONFIG_TABLE)
                        .username(MYSQL_USERNAME)
                        .password(MYSQL_PASSWORD)
                        .serverTimeZone(MYSQL_SERVER_TIME_ZONE)
                        .serverId(5401)
                        .startupOptions(StartupOptions.initial())
                        .debeziumProperties(debeziumProperties)
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .build(),
                "mysql-dim-config-source"
        );

        BroadcastStream<String> broadcastConfigStream = configStream.broadcast(DIM_CONFIG_DESCRIPTOR);

        SingleOutputStreamOperator<String> dimWriteResultStream = odsDbStream
                .connect(broadcastConfigStream)
                .process(new DimBroadcastProcessFunction())
                .name("dim-config-broadcast-and-hbase-writer");

        // 打印维表写入结果，便于开发阶段观察命中的维表和操作类型。
        dimWriteResultStream.print("dim");

        env.execute("dim_db_sink");
    }

    /**
     * 维表配置广播处理函数。
     * 主流命中维表配置后直接写HBase；非维表数据不输出、不写入。
     */
    private static class DimBroadcastProcessFunction
            extends BroadcastProcessFunction<String, String, String> {

        private transient Connection hbaseConnection;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            org.apache.hadoop.conf.Configuration hbaseConfig = HBaseConfiguration.create();
            hbaseConfig.set("hbase.zookeeper.quorum", "node101");
            hbaseConnection = ConnectionFactory.createConnection(hbaseConfig);
        }

        @Override
        public void processElement(String value, ReadOnlyContext context, Collector<String> collector) throws Exception {
            JSONObject root;
            try {
                root = JSON.parseObject(value);
            } catch (Exception e) {
                return;
            }

            String sourceTable = getSourceTable(root);
            if (sourceTable == null || sourceTable.length() == 0) {
                return;
            }

            ReadOnlyBroadcastState<String, TableProcessDim> configState =
                    context.getBroadcastState(DIM_CONFIG_DESCRIPTOR);
            TableProcessDim config = configState.get(sourceTable);
            if (config == null) {
                return;
            }

            String op = root.getString("op");
            if ("d".equals(op)) {
                JSONObject before = root.getJSONObject("before");
                deleteDimRow(config, before);
                collector.collect("delete " + config.sinkTable + " rowkey=" + getRowKey(config, before));
                return;
            }

            if ("c".equals(op) || "r".equals(op) || "u".equals(op)) {
                JSONObject after = root.getJSONObject("after");
                writeDimRow(config, after);
                collector.collect("upsert " + config.sinkTable + " rowkey=" + getRowKey(config, after));
            }
        }

        @Override
        public void processBroadcastElement(String value, Context context, Collector<String> collector) throws Exception {
            JSONObject root;
            try {
                root = JSON.parseObject(value);
            } catch (Exception e) {
                return;
            }

            String op = root.getString("op");
            JSONObject configJson = "d".equals(op) ? root.getJSONObject("before") : root.getJSONObject("after");
            if (configJson == null) {
                return;
            }

            String sourceTable = configJson.getString("source_table");
            if (sourceTable == null || sourceTable.length() == 0) {
                return;
            }

            BroadcastState<String, TableProcessDim> configState =
                    context.getBroadcastState(DIM_CONFIG_DESCRIPTOR);
            if ("d".equals(op)) {
                configState.remove(sourceTable);
                collector.collect("remove dim config source_table=" + sourceTable);
                return;
            }

            TableProcessDim config = TableProcessDim.fromJson(configJson);
            if (!"dim".equalsIgnoreCase(config.sinkType)) {
                configState.remove(sourceTable);
                return;
            }

            ensureHBaseTable(config);
            configState.put(sourceTable, config);
            collector.collect("put dim config source_table=" + sourceTable + ", sink_table=" + config.sinkTable);
        }

        @Override
        public void close() throws Exception {
            if (hbaseConnection != null) {
                hbaseConnection.close();
            }
        }

        /**
         * 自动创建HBase namespace和表；表已存在时确保列族存在。
         */
        private void ensureHBaseTable(TableProcessDim config) throws Exception {
            try (Admin admin = hbaseConnection.getAdmin()) {
                try {
                    admin.getNamespaceDescriptor(HBASE_NAMESPACE);
                } catch (NamespaceNotFoundException e) {
                    admin.createNamespace(NamespaceDescriptor.create(HBASE_NAMESPACE).build());
                }

                TableName tableName = TableName.valueOf(HBASE_NAMESPACE, config.sinkTable);
                byte[] family = Bytes.toBytes(config.sinkFamily);
                if (!admin.tableExists(tableName)) {
                    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                            .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(family).build())
                            .build();
                    admin.createTable(tableDescriptor);
                    return;
                }

                TableDescriptor tableDescriptor = admin.getDescriptor(tableName);
                if (!tableDescriptor.hasColumnFamily(family)) {
                    admin.addColumnFamily(
                            tableName,
                            ColumnFamilyDescriptorBuilder.newBuilder(family).build()
                    );
                }
            }
        }

        /**
         * 写入维表数据，rowkey字段只作为行键，不重复写入列族。
         */
        private void writeDimRow(TableProcessDim config, JSONObject data) throws Exception {
            if (data == null) {
                return;
            }

            String rowKey = getRowKey(config, data);
            if (rowKey == null || rowKey.length() == 0) {
                return;
            }

            Put put = new Put(Bytes.toBytes(rowKey));
            Set<String> sinkColumnSet = parseSinkColumns(config.sinkColumns);
            for (String column : data.keySet()) {
                if (config.sinkRowKey.equals(column)) {
                    continue;
                }
                if (sinkColumnSet != null && !sinkColumnSet.contains(column)) {
                    continue;
                }

                Object columnValue = data.get(column);
                if (columnValue == null) {
                    continue;
                }

                put.addColumn(
                        Bytes.toBytes(config.sinkFamily),
                        Bytes.toBytes(column),
                        Bytes.toBytes(String.valueOf(columnValue))
                );
            }

            if (put.isEmpty()) {
                return;
            }

            try (Table table = hbaseConnection.getTable(TableName.valueOf(HBASE_NAMESPACE, config.sinkTable))) {
                table.put(put);
            }
        }

        /**
         * 删除维表行，删除事件取before中的rowkey字段。
         */
        private void deleteDimRow(TableProcessDim config, JSONObject data) throws Exception {
            String rowKey = getRowKey(config, data);
            if (rowKey == null || rowKey.length() == 0) {
                return;
            }

            try (Table table = hbaseConnection.getTable(TableName.valueOf(HBASE_NAMESPACE, config.sinkTable))) {
                table.delete(new Delete(Bytes.toBytes(rowKey)));
            }
        }

        private String getSourceTable(JSONObject root) {
            JSONObject source = root.getJSONObject("source");
            if (source != null) {
                return source.getString("table");
            }
            return root.getString("table");
        }

        private String getRowKey(TableProcessDim config, JSONObject data) {
            if (data == null) {
                return null;
            }
            Object rowKeyValue = data.get(config.sinkRowKey);
            return rowKeyValue == null ? null : String.valueOf(rowKeyValue);
        }

        private Set<String> parseSinkColumns(String sinkColumns) {
            if (sinkColumns == null || sinkColumns.trim().length() == 0) {
                return null;
            }

            Set<String> columnSet = new HashSet<String>();
            String[] columns = sinkColumns.split(",");
            for (String column : columns) {
                if (column != null && column.trim().length() > 0) {
                    columnSet.add(column.trim());
                }
            }
            return columnSet;
        }
    }

    /**
     * MySQL配置表 table_process_dim 对应的内存配置对象。
     */
    public static class TableProcessDim implements Serializable {
        public String sourceTable;
        public String sinkTable;
        public String sinkFamily;
        public String sinkRowKey;
        public String sinkColumns;
        public String sinkType;

        public static TableProcessDim fromJson(JSONObject jsonObject) {
            TableProcessDim config = new TableProcessDim();
            config.sourceTable = jsonObject.getString("source_table");
            config.sinkTable = jsonObject.getString("sink_table");
            config.sinkFamily = defaultIfBlank(jsonObject.getString("sink_family"), "info");
            config.sinkRowKey = defaultIfBlank(jsonObject.getString("sink_row_key"), "id");
            config.sinkColumns = jsonObject.getString("sink_columns");
            config.sinkType = defaultIfBlank(jsonObject.getString("sink_type"), "dim");
            return config;
        }

        private static String defaultIfBlank(String value, String defaultValue) {
            return value == null || value.trim().length() == 0 ? defaultValue : value.trim();
        }
    }
}
