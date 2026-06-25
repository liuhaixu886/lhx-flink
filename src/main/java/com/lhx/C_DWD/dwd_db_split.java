package com.lhx.C_DWD;

import com.alibaba.fastjson.JSON;
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
 * DWD层业务事实表级分流作业。
 * 从ods_db读取Debezium JSON，做基础清洗后把事实表变更写入对应DWD主题。
 *
 * @author 刘海旭
 * @date 2026/05/26
 */
public class dwd_db_split {

    /** Kafka集群地址。 */
    private static final String KAFKA_BOOTSTRAP_SERVERS = "node101:9092,node102:9092,node103:9092";

    /** ODS层业务数据入口主题。 */
    private static final String SOURCE_TOPIC_ODS_DB = "ods_db";

    /** DWD层业务事实输出主题。 */
    private static final String SINK_TOPIC_CART_INFO = "dwd_trade_cart_info";
    private static final String SINK_TOPIC_ORDER_INFO = "dwd_trade_order_info";
    private static final String SINK_TOPIC_ORDER_DETAIL = "dwd_trade_order_detail";
    private static final String SINK_TOPIC_ORDER_DETAIL_ACTIVITY = "dwd_trade_order_detail_activity";
    private static final String SINK_TOPIC_ORDER_DETAIL_COUPON = "dwd_trade_order_detail_coupon";
    private static final String SINK_TOPIC_PAYMENT_INFO = "dwd_trade_payment_info";
    private static final String SINK_TOPIC_ORDER_REFUND_INFO = "dwd_trade_order_refund_info";
    private static final String SINK_TOPIC_REFUND_PAYMENT = "dwd_trade_refund_payment";
    private static final String SINK_TOPIC_ORDER_STATUS_LOG = "dwd_trade_order_status_log";
    private static final String SINK_TOPIC_COUPON_USE = "dwd_tool_coupon_use";
    private static final String SINK_TOPIC_FAVOR_INFO = "dwd_interaction_favor_info";
    private static final String SINK_TOPIC_COMMENT_INFO = "dwd_interaction_comment_info";
    private static final String SINK_TOPIC_DIRTY_DATA = "dirty_data";

    /** 当前作业的Kafka消费者组。 */
    private static final String CONSUMER_GROUP_ID = "dwd_db_split";

    /** 侧输出流标签：各业务事实主题和脏数据主题。 */
    private static final OutputTag<String> ORDER_INFO_TAG = new OutputTag<String>("order-info") {
    };
    private static final OutputTag<String> ORDER_DETAIL_TAG = new OutputTag<String>("order-detail") {
    };
    private static final OutputTag<String> ORDER_DETAIL_ACTIVITY_TAG = new OutputTag<String>("order-detail-activity") {
    };
    private static final OutputTag<String> ORDER_DETAIL_COUPON_TAG = new OutputTag<String>("order-detail-coupon") {
    };
    private static final OutputTag<String> PAYMENT_INFO_TAG = new OutputTag<String>("payment-info") {
    };
    private static final OutputTag<String> ORDER_REFUND_INFO_TAG = new OutputTag<String>("order-refund-info") {
    };
    private static final OutputTag<String> REFUND_PAYMENT_TAG = new OutputTag<String>("refund-payment") {
    };
    private static final OutputTag<String> ORDER_STATUS_LOG_TAG = new OutputTag<String>("order-status-log") {
    };
    private static final OutputTag<String> COUPON_USE_TAG = new OutputTag<String>("coupon-use") {
    };
    private static final OutputTag<String> FAVOR_INFO_TAG = new OutputTag<String>("favor-info") {
    };
    private static final OutputTag<String> COMMENT_INFO_TAG = new OutputTag<String>("comment-info") {
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
                SOURCE_TOPIC_ODS_DB,
                new SimpleStringSchema(StandardCharsets.UTF_8),
                kafkaConsumerProperties
        );
        kafkaConsumer.setStartFromGroupOffsets();

        DataStreamSource<String> odsDbStream = env.addSource(kafkaConsumer, "kafka-ods-db-source");

        SingleOutputStreamOperator<String> cartInfoStream = odsDbStream.process(
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

                        String sourceTable = getSourceTable(root);
                        String op = root.getString("op");
                        if (isBlank(sourceTable) || isBlank(op)) {
                            context.output(DIRTY_DATA_TAG, value);
                            return;
                        }

                        // 删除事件第一版不进入DWD明细，后续如需删除事实可单独设计撤回流。
                        if ("d".equals(op)) {
                            return;
                        }

                        if (!"c".equals(op) && !"r".equals(op) && !"u".equals(op)) {
                            context.output(DIRTY_DATA_TAG, value);
                            return;
                        }

                        JSONObject after = root.getJSONObject("after");
                        if (after == null) {
                            context.output(DIRTY_DATA_TAG, value);
                            return;
                        }

                        // DWD事实表第一版要求业务主键id存在，避免下游无法定位明细。
                        if (isBlank(after.getString("id"))) {
                            context.output(DIRTY_DATA_TAG, value);
                            return;
                        }

                        after.put("_source_table", sourceTable);
                        after.put("_op", op);
                        after.put("_ts_ms", root.getLong("ts_ms"));

                        String result = after.toJSONString();
                        if ("cart_info".equals(sourceTable)) {
                            collector.collect(result);
                        } else if ("order_info".equals(sourceTable)) {
                            context.output(ORDER_INFO_TAG, result);
                        } else if ("order_detail".equals(sourceTable)) {
                            context.output(ORDER_DETAIL_TAG, result);
                        } else if ("order_detail_activity".equals(sourceTable)) {
                            context.output(ORDER_DETAIL_ACTIVITY_TAG, result);
                        } else if ("order_detail_coupon".equals(sourceTable)) {
                            context.output(ORDER_DETAIL_COUPON_TAG, result);
                        } else if ("payment_info".equals(sourceTable)) {
                            context.output(PAYMENT_INFO_TAG, result);
                        } else if ("order_refund_info".equals(sourceTable)) {
                            context.output(ORDER_REFUND_INFO_TAG, result);
                        } else if ("refund_payment".equals(sourceTable)) {
                            context.output(REFUND_PAYMENT_TAG, result);
                        } else if ("order_status_log".equals(sourceTable)) {
                            context.output(ORDER_STATUS_LOG_TAG, result);
                        } else if ("coupon_use".equals(sourceTable)) {
                            context.output(COUPON_USE_TAG, result);
                        } else if ("favor_info".equals(sourceTable)) {
                            context.output(FAVOR_INFO_TAG, result);
                        } else if ("comment_info".equals(sourceTable)) {
                            context.output(COMMENT_INFO_TAG, result);
                        }
                    }
                }
        ).name("clean-and-split-db");

        DataStream<String> orderInfoStream = cartInfoStream.getSideOutput(ORDER_INFO_TAG);
        DataStream<String> orderDetailStream = cartInfoStream.getSideOutput(ORDER_DETAIL_TAG);
        DataStream<String> orderDetailActivityStream = cartInfoStream.getSideOutput(ORDER_DETAIL_ACTIVITY_TAG);
        DataStream<String> orderDetailCouponStream = cartInfoStream.getSideOutput(ORDER_DETAIL_COUPON_TAG);
        DataStream<String> paymentInfoStream = cartInfoStream.getSideOutput(PAYMENT_INFO_TAG);
        DataStream<String> orderRefundInfoStream = cartInfoStream.getSideOutput(ORDER_REFUND_INFO_TAG);
        DataStream<String> refundPaymentStream = cartInfoStream.getSideOutput(REFUND_PAYMENT_TAG);
        DataStream<String> orderStatusLogStream = cartInfoStream.getSideOutput(ORDER_STATUS_LOG_TAG);
        DataStream<String> couponUseStream = cartInfoStream.getSideOutput(COUPON_USE_TAG);
        DataStream<String> favorInfoStream = cartInfoStream.getSideOutput(FAVOR_INFO_TAG);
        DataStream<String> commentInfoStream = cartInfoStream.getSideOutput(COMMENT_INFO_TAG);
        DataStream<String> dirtyDataStream = cartInfoStream.getSideOutput(DIRTY_DATA_TAG);

        Properties kafkaProducerProperties = new Properties();
        kafkaProducerProperties.setProperty("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);

        cartInfoStream.addSink(createKafkaProducer(SINK_TOPIC_CART_INFO, kafkaProducerProperties))
                .name("kafka-dwd-cart-info-sink");
        orderInfoStream.addSink(createKafkaProducer(SINK_TOPIC_ORDER_INFO, kafkaProducerProperties))
                .name("kafka-dwd-order-info-sink");
        orderDetailStream.addSink(createKafkaProducer(SINK_TOPIC_ORDER_DETAIL, kafkaProducerProperties))
                .name("kafka-dwd-order-detail-sink");
        orderDetailActivityStream.addSink(createKafkaProducer(SINK_TOPIC_ORDER_DETAIL_ACTIVITY, kafkaProducerProperties))
                .name("kafka-dwd-order-detail-activity-sink");
        orderDetailCouponStream.addSink(createKafkaProducer(SINK_TOPIC_ORDER_DETAIL_COUPON, kafkaProducerProperties))
                .name("kafka-dwd-order-detail-coupon-sink");
        paymentInfoStream.addSink(createKafkaProducer(SINK_TOPIC_PAYMENT_INFO, kafkaProducerProperties))
                .name("kafka-dwd-payment-info-sink");
        orderRefundInfoStream.addSink(createKafkaProducer(SINK_TOPIC_ORDER_REFUND_INFO, kafkaProducerProperties))
                .name("kafka-dwd-order-refund-info-sink");
        refundPaymentStream.addSink(createKafkaProducer(SINK_TOPIC_REFUND_PAYMENT, kafkaProducerProperties))
                .name("kafka-dwd-refund-payment-sink");
        orderStatusLogStream.addSink(createKafkaProducer(SINK_TOPIC_ORDER_STATUS_LOG, kafkaProducerProperties))
                .name("kafka-dwd-order-status-log-sink");
        couponUseStream.addSink(createKafkaProducer(SINK_TOPIC_COUPON_USE, kafkaProducerProperties))
                .name("kafka-dwd-coupon-use-sink");
        favorInfoStream.addSink(createKafkaProducer(SINK_TOPIC_FAVOR_INFO, kafkaProducerProperties))
                .name("kafka-dwd-favor-info-sink");
        commentInfoStream.addSink(createKafkaProducer(SINK_TOPIC_COMMENT_INFO, kafkaProducerProperties))
                .name("kafka-dwd-comment-info-sink");
        dirtyDataStream.addSink(createKafkaProducer(SINK_TOPIC_DIRTY_DATA, kafkaProducerProperties))
                .name("kafka-dirty-data-sink");

        env.execute("dwd_db_split");
    }

    private static FlinkKafkaProducer<String> createKafkaProducer(String topic, Properties properties) {
        return new FlinkKafkaProducer<String>(
                topic,
                new SimpleStringSchema(StandardCharsets.UTF_8),
                properties
        );
    }

    private static String getSourceTable(JSONObject root) {
        JSONObject source = root.getJSONObject("source");
        if (source != null) {
            return source.getString("table");
        }
        return root.getString("table");
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().length() == 0;
    }
}
