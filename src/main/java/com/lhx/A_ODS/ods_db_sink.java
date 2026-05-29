package com.lhx.A_ODS;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * ods_db_sink
 *
 * @author 刘海旭
 * @date 2026/05/26 15:39
 */
public class ods_db_sink {

    /** Kafka 集群地址。 */
    private static final String KAFKA_BOOTSTRAP_SERVERS = "node101:9092,node102:9092,node103:9092";

    /** CDC 原始业务数据主题。 */
    private static final String SOURCE_TOPIC_DB = "topic_db";

    /** ODS 层业务数据落地主题。 */
    private static final String SINK_TOPIC_ODS_DB = "ods_db";

    /** ODS 层统一脏数据主题。 */
    private static final String SINK_TOPIC_DIRTY_DATA = "dirty_data";

    /** 当前作业的 Kafka 消费者组。 */
    private static final String CONSUMER_GROUP_ID = "ods_db_sink";

    /** 非法 JSON 数据侧输出流标签。 */
    private static final OutputTag<String> DIRTY_DATA_TAG = new OutputTag<String>("dirty-data") {
    };

    public static void main(String[] args) throws Exception {
        // 创建 Flink 流处理环境，当前 ODS 数据转存作业先使用单并行度便于本地观察输出。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 开启 Checkpoint，让 Kafka 消费位点随状态一起提交，失败后可以从已确认位置恢复。
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        env.setStateBackend(new HashMapStateBackend());

        // 保留外部 Checkpoint，任务取消后仍可基于已有状态继续恢复。
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

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                SOURCE_TOPIC_DB,
                new SimpleStringSchema(StandardCharsets.UTF_8),
                kafkaConsumerProperties
        );

        // 没有历史消费位点时，从 topic_db 的最早数据开始消费；已有位点时按消费者组位点继续。
        kafkaConsumer.setStartFromGroupOffsets();

        // 实时读取 topic_db 中的 Debezium JSON 字符串。
        DataStreamSource<String> topicDbStream = env.addSource(kafkaConsumer, "kafka-topic-db-source");

        // 打印读取到的数据，方便开发阶段确认 CDC 数据格式和流量。
        topicDbStream.print("topic_db");

        // ODS 层只做 JSON 格式合法性校验，不做字段清洗、业务过滤、表拆分等 DWD 层工作。
        SingleOutputStreamOperator<String> validJsonStream = topicDbStream.process(
                new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context context, Collector<String> collector) {
                        if (isValidJsonObject(value)) {
                            collector.collect(value);
                        } else {
                            context.output(DIRTY_DATA_TAG, value);
                        }
                    }
                }
        ).name("check-json-format");

        // 从侧输出流中取出非法 JSON，统一写入 dirty_data，便于后续排查问题数据。
        DataStream<String> dirtyDataStream = validJsonStream.getSideOutput(DIRTY_DATA_TAG);

        Properties kafkaProducerProperties = new Properties();
        kafkaProducerProperties.setProperty("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);

        // 合法 JSON 写入 ods_db，作为后续 DWD/DIM 分流的统一入口。
        validJsonStream.addSink(
                new FlinkKafkaProducer<>(
                        SINK_TOPIC_ODS_DB,
                        new SimpleStringSchema(StandardCharsets.UTF_8),
                        kafkaProducerProperties
                )
        ).name("kafka-ods-db-sink");

        // 非法 JSON 或空字符串写入 dirty_data，不污染 ods_db。
        dirtyDataStream.addSink(
                new FlinkKafkaProducer<>(
                        SINK_TOPIC_DIRTY_DATA,
                        new SimpleStringSchema(StandardCharsets.UTF_8),
                        kafkaProducerProperties
                )
        ).name("kafka-dirty-data-sink");

        env.execute("ods_db_sink");
    }

    /**
     * 判断消息是否为合法 JSON 对象。
     * ODS 层只判断格式是否能解析，不判断业务字段是否完整或含义是否正确。
     */
    private static boolean isValidJsonObject(String value) {
        if (value == null || value.trim().length() == 0) {
            return false;
        }

        try {
            JSON.parseObject(value);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
