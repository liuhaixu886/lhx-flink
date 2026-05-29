package com.lhx.A_ODS;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * ods_db_mysql_to_kafka
 *
 * @author 刘海旭
 * @date 2026/05/26 14:46
 */
public class ods_db_mysql_cdc_kafka {

    /** MySQL CDC 数据源配置。 */
    private static final String MYSQL_HOSTNAME = "node101";
    private static final int MYSQL_PORT = 3306;
    private static final String MYSQL_DATABASE = "gmall";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "123456";
    private static final String MYSQL_SERVER_TIME_ZONE = "Asia/Shanghai";

    /** ODS 业务数据写入 Kafka 的配置。 */
    private static final String KAFKA_BOOTSTRAP_SERVERS = "node101:9092,node102:9092,node103:9092";
    private static final String KAFKA_TOPIC_DB = "topic_db";

    public static void main(String[] args) throws Exception {
        // ODS CDC 作业先使用单并行度，保证源端读取和写入顺序稳定。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 开启 Checkpoint，用于保存 CDC 的 binlog 位点，故障恢复时避免从头重新采集。
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        env.setStateBackend(new HashMapStateBackend());

        // 保留外部 Checkpoint，任务取消后仍可从已保存的 CDC 位点恢复。
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(60000L);
        checkpointConfig.setMinPauseBetweenCheckpoints(3000L);
        checkpointConfig.setTolerableCheckpointFailureNumber(3);
        checkpointConfig.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        // Debezium 时区与 MySQL 服务端保持一致，避免时间字段发生偏移。
        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("database.serverTimezone", MYSQL_SERVER_TIME_ZONE);
        // MySQL decimal 类型默认会被 Debezium 编码成二进制 Base64，ODS 层统一转成字符串金额，避免下游出现 CSoY、AA== 这类值。
        debeziumProperties.setProperty("decimal.handling.mode", "string");

        // 采集 gmall 库所有表：首次启动先做全量快照，然后持续读取 binlog 增量变更。
        DataStreamSource<String> mysqlCdcSource = env.addSource(
                MySqlSource.<String>builder()
                        .hostname(MYSQL_HOSTNAME)
                        .port(MYSQL_PORT)
                        .databaseList(MYSQL_DATABASE)
                        .username(MYSQL_USERNAME)
                        .password(MYSQL_PASSWORD)
                        .serverTimeZone(MYSQL_SERVER_TIME_ZONE)
                        .startupOptions(StartupOptions.initial())
                        .debeziumProperties(debeziumProperties)
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .build(),
                "mysql-cdc-source"
        );

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);

        // 将原始 Debezium JSON 写入 topic_db，作为 ODS 层业务变更日志流。
        mysqlCdcSource.addSink(
                new FlinkKafkaProducer<>(
                        KAFKA_TOPIC_DB,
                        new SimpleStringSchema(StandardCharsets.UTF_8),
                        kafkaProperties
                )
        ).name("kafka-topic-db-sink");

        env.execute("ods_db_mysql_to_kafka");
    }
}
