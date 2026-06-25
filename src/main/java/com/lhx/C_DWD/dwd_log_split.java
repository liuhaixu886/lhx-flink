package com.lhx.C_DWD;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
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
 * DWD层日志明细分流作业。
 * 从ods_log读取日志，做基础格式清洗后拆分页面、启动、曝光、动作、错误明细。
 *
 * @author 刘海旭
 * @date 2026/05/26
 */
public class dwd_log_split {

    /** Kafka集群地址。 */
    private static final String KAFKA_BOOTSTRAP_SERVERS = "node101:9092,node102:9092,node103:9092";

    /** ODS层日志入口主题。 */
    private static final String SOURCE_TOPIC_ODS_LOG = "ods_log";

    /** DWD层日志明细输出主题。 */
    private static final String SINK_TOPIC_PAGE_LOG = "dwd_traffic_page_log";
    private static final String SINK_TOPIC_START_LOG = "dwd_traffic_start_log";
    private static final String SINK_TOPIC_DISPLAY_LOG = "dwd_traffic_display_log";
    private static final String SINK_TOPIC_ACTION_LOG = "dwd_traffic_action_log";
    private static final String SINK_TOPIC_ERROR_LOG = "dwd_traffic_error_log";
    private static final String SINK_TOPIC_DIRTY_DATA = "dirty_data";

    /** 当前作业的Kafka消费者组。 */
    private static final String CONSUMER_GROUP_ID = "dwd_log_split";

    /** 侧输出流标签：启动、曝光、动作、错误和脏数据。 */
    private static final OutputTag<String> START_LOG_TAG = new OutputTag<String>("start-log") {
    };
    private static final OutputTag<String> DISPLAY_LOG_TAG = new OutputTag<String>("display-log") {
    };
    private static final OutputTag<String> ACTION_LOG_TAG = new OutputTag<String>("action-log") {
    };
    private static final OutputTag<String> ERROR_LOG_TAG = new OutputTag<String>("error-log") {
    };
    private static final OutputTag<String> DIRTY_DATA_TAG = new OutputTag<String>("dirty-data") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Checkpoint用于保存Kafka消费位点，失败重启后可以继续从已确认位置消费。
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
                SOURCE_TOPIC_ODS_LOG,
                new SimpleStringSchema(StandardCharsets.UTF_8),
                kafkaConsumerProperties
        );
        kafkaConsumer.setStartFromGroupOffsets();

        DataStreamSource<String> odsLogStream = env.addSource(kafkaConsumer, "kafka-ods-log-source");

        SingleOutputStreamOperator<String> pageLogStream = odsLogStream.process(
                new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context context, Collector<String> collector) {
                        JSONObject root;
                        try {
                            root = JSON.parseObject(value);
                        } catch (Exception e) {
                            context.output(DIRTY_DATA_TAG, value);
                            return;
                        }

                        // DWD层做基础清洗：日志必须有公共信息、时间戳和设备唯一标识。
                        JSONObject common = root.getJSONObject("common");
                        Long ts = root.getLong("ts");
                        if (common == null || ts == null || isBlank(common.getString("mid"))) {
                            context.output(DIRTY_DATA_TAG, value);
                            return;
                        }

                        JSONObject page = root.getJSONObject("page");
                        JSONObject start = root.getJSONObject("start");
                        if (page == null && start == null) {
                            context.output(DIRTY_DATA_TAG, value);
                            return;
                        }

                        if (start != null) {
                            context.output(START_LOG_TAG, root.toJSONString());
                        }

                        if (page != null) {
                            collector.collect(root.toJSONString());
                        }

                        // 曝光日志一条原始日志可能包含多条曝光明细，需要拆成多条写出。
                        JSONArray displays = root.getJSONArray("displays");
                        if (displays != null) {
                            for (int i = 0; i < displays.size(); i++) {
                                JSONObject display = displays.getJSONObject(i);
                                display.put("common", common);
                                display.put("page", page);
                                display.put("ts", ts);
                                context.output(DISPLAY_LOG_TAG, display.toJSONString());
                            }
                        }

                        // 动作日志一条原始日志可能包含多条动作明细，需要拆成多条写出。
                        JSONArray actions = root.getJSONArray("actions");
                        if (actions != null) {
                            for (int i = 0; i < actions.size(); i++) {
                                JSONObject action = actions.getJSONObject(i);
                                action.put("common", common);
                                action.put("page", page);
                                action.put("ts", ts);
                                context.output(ACTION_LOG_TAG, action.toJSONString());
                            }
                        }

                        JSONObject err = root.getJSONObject("err");
                        if (err != null) {
                            err.put("common", common);
                            err.put("page", page);
                            err.put("ts", ts);
                            context.output(ERROR_LOG_TAG, err.toJSONString());
                        }
                    }
                }
        ).name("clean-and-split-log");

        DataStream<String> startLogStream = pageLogStream.getSideOutput(START_LOG_TAG);
        DataStream<String> displayLogStream = pageLogStream.getSideOutput(DISPLAY_LOG_TAG);
        DataStream<String> actionLogStream = pageLogStream.getSideOutput(ACTION_LOG_TAG);
        DataStream<String> errorLogStream = pageLogStream.getSideOutput(ERROR_LOG_TAG);
        DataStream<String> dirtyDataStream = pageLogStream.getSideOutput(DIRTY_DATA_TAG);

        Properties kafkaProducerProperties = new Properties();
        kafkaProducerProperties.setProperty("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);

        pageLogStream.addSink(createKafkaProducer(SINK_TOPIC_PAGE_LOG, kafkaProducerProperties))
                .name("kafka-dwd-page-log-sink");
        startLogStream.addSink(createKafkaProducer(SINK_TOPIC_START_LOG, kafkaProducerProperties))
                .name("kafka-dwd-start-log-sink");
        displayLogStream.addSink(createKafkaProducer(SINK_TOPIC_DISPLAY_LOG, kafkaProducerProperties))
                .name("kafka-dwd-display-log-sink");
        actionLogStream.addSink(createKafkaProducer(SINK_TOPIC_ACTION_LOG, kafkaProducerProperties))
                .name("kafka-dwd-action-log-sink");
        errorLogStream.addSink(createKafkaProducer(SINK_TOPIC_ERROR_LOG, kafkaProducerProperties))
                .name("kafka-dwd-error-log-sink");
        dirtyDataStream.addSink(createKafkaProducer(SINK_TOPIC_DIRTY_DATA, kafkaProducerProperties))
                .name("kafka-dirty-data-sink");

        env.execute("dwd_log_split");
    }

    private static FlinkKafkaProducer<String> createKafkaProducer(String topic, Properties properties) {
        return new FlinkKafkaProducer<String>(
                topic,
                new SimpleStringSchema(StandardCharsets.UTF_8),
                properties
        );
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().length() == 0;
    }
}
