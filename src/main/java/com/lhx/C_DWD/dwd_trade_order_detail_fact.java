package com.lhx.C_DWD;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * DWD交易域订单明细事实宽流作业。
 * 基于表级DWD主题做状态关联，输出可支撑GMV、省份交易、商品排行的订单明细事实。
 *
 * @author 刘海旭
 * @date 2026/05/26
 */
public class dwd_trade_order_detail_fact {

    /** Kafka集群地址。 */
    private static final String KAFKA_BOOTSTRAP_SERVERS = "node101:9092,node102:9092,node103:9092";

    /** 表级DWD输入主题。 */
    private static final String SOURCE_TOPIC_ORDER_DETAIL = "dwd_trade_order_detail";
    private static final String SOURCE_TOPIC_ORDER_INFO = "dwd_trade_order_info";
    private static final String SOURCE_TOPIC_ORDER_DETAIL_ACTIVITY = "dwd_trade_order_detail_activity";
    private static final String SOURCE_TOPIC_ORDER_DETAIL_COUPON = "dwd_trade_order_detail_coupon";

    /** 订单明细事实输出主题。 */
    private static final String SINK_TOPIC_ORDER_DETAIL_FACT = "dwd_trade_order_detail_fact";
    private static final String SINK_TOPIC_DIRTY_DATA = "dirty_data";

    /** 当前作业的Kafka消费者组。 */
    private static final String CONSUMER_GROUP_ID = "dwd_trade_order_detail_fact";

    /** 状态保留30分钟，覆盖订单相关表常见乱序到达间隔。 */
    private static final int STATE_TTL_MINUTES = 30;

    /** 侧输出流标签：脏数据。 */
    private static final OutputTag<String> DIRTY_DATA_TAG = new OutputTag<String>("dirty-data") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Checkpoint用于保存Kafka消费位点和关联状态，失败重启后可以继续处理未完成关联的数据。
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

        SingleOutputStreamOperator<JSONObject> orderDetailStream = readAndCleanJson(
                env,
                SOURCE_TOPIC_ORDER_DETAIL,
                kafkaConsumerProperties,
                "order-detail"
        );
        SingleOutputStreamOperator<JSONObject> orderInfoStream = readAndCleanJson(
                env,
                SOURCE_TOPIC_ORDER_INFO,
                kafkaConsumerProperties,
                "order-info"
        );
        SingleOutputStreamOperator<JSONObject> activityStream = readAndCleanJson(
                env,
                SOURCE_TOPIC_ORDER_DETAIL_ACTIVITY,
                kafkaConsumerProperties,
                "order-detail-activity"
        );
        SingleOutputStreamOperator<JSONObject> couponStream = readAndCleanJson(
                env,
                SOURCE_TOPIC_ORDER_DETAIL_COUPON,
                kafkaConsumerProperties,
                "order-detail-coupon"
        );

        // 第一步：订单明细和订单主表按order_id关联，生成基础订单明细事实。
        SingleOutputStreamOperator<JSONObject> baseFactStream = orderDetailStream
                .keyBy(json -> json.getString("order_id"))
                .connect(orderInfoStream.keyBy(json -> json.getString("id")))
                .process(new OrderDetailOrderInfoJoinFunction())
                .name("join-order-detail-and-order-info");

        // 第二步：按订单明细id补充活动信息；没有活动时也保留基础事实。
        SingleOutputStreamOperator<JSONObject> activityFactStream = baseFactStream
                .keyBy(json -> json.getString("id"))
                .connect(activityStream.keyBy(json -> json.getString("order_detail_id")))
                .process(new ActivityJoinFunction())
                .name("join-order-detail-activity");

        // 第三步：按订单明细id补充优惠券信息；没有优惠券时也保留订单事实。
        SingleOutputStreamOperator<JSONObject> factStream = activityFactStream
                .keyBy(json -> json.getString("id"))
                .connect(couponStream.keyBy(json -> json.getString("order_detail_id")))
                .process(new CouponJoinFunction())
                .name("join-order-detail-coupon");

        DataStream<String> dirtyDataStream = orderDetailStream.getSideOutput(DIRTY_DATA_TAG)
                .union(orderInfoStream.getSideOutput(DIRTY_DATA_TAG))
                .union(activityStream.getSideOutput(DIRTY_DATA_TAG))
                .union(couponStream.getSideOutput(DIRTY_DATA_TAG))
                .union(baseFactStream.getSideOutput(DIRTY_DATA_TAG))
                .union(activityFactStream.getSideOutput(DIRTY_DATA_TAG))
                .union(factStream.getSideOutput(DIRTY_DATA_TAG));

        Properties kafkaProducerProperties = new Properties();
        kafkaProducerProperties.setProperty("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);

        factStream
                .map((MapFunction<JSONObject, String>) value -> value.toJSONString())
                .addSink(createKafkaProducer(SINK_TOPIC_ORDER_DETAIL_FACT, kafkaProducerProperties))
                .name("kafka-dwd-order-detail-fact-sink");

        dirtyDataStream.addSink(createKafkaProducer(SINK_TOPIC_DIRTY_DATA, kafkaProducerProperties))
                .name("kafka-dirty-data-sink");

        env.execute("dwd_trade_order_detail_fact");
    }

    /**
     * 读取Kafka主题并解析为JSON对象，非法JSON统一旁路到dirty_data。
     */
    private static SingleOutputStreamOperator<JSONObject> readAndCleanJson(
            StreamExecutionEnvironment env,
            String topic,
            Properties kafkaConsumerProperties,
            String sourceName) {
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(
                topic,
                new SimpleStringSchema(StandardCharsets.UTF_8),
                kafkaConsumerProperties
        );
        kafkaConsumer.setStartFromGroupOffsets();

        DataStreamSource<String> sourceStream = env.addSource(
                kafkaConsumer,
                "kafka-" + sourceName + "-source"
        );

        return sourceStream.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context context, Collector<JSONObject> collector) {
                if (isBlank(value)) {
                    context.output(DIRTY_DATA_TAG, value);
                    return;
                }

                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    if (isInvalidSourceRecord(sourceName, jsonObject)) {
                        context.output(DIRTY_DATA_TAG, value);
                        return;
                    }
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(DIRTY_DATA_TAG, value);
                }
            }
        }).name("parse-" + sourceName + "-json");
    }

    /**
     * 订单明细和订单主表状态关联函数。
     */
    private static class OrderDetailOrderInfoJoinFunction
            extends KeyedCoProcessFunction<String, JSONObject, JSONObject, JSONObject> {

        private transient ValueState<JSONObject> orderDetailState;
        private transient ValueState<JSONObject> orderInfoState;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            orderDetailState = getRuntimeContext().getState(createJsonStateDescriptor("order-detail-state"));
            orderInfoState = getRuntimeContext().getState(createJsonStateDescriptor("order-info-state"));
        }

        @Override
        public void processElement1(JSONObject orderDetail, Context context, Collector<JSONObject> collector) throws Exception {
            if (isInvalidOrderDetail(orderDetail)) {
                context.output(DIRTY_DATA_TAG, orderDetail.toJSONString());
                return;
            }

            orderDetailState.update(orderDetail);
            JSONObject orderInfo = orderInfoState.value();
            if (orderInfo != null) {
                collector.collect(buildBaseFact(orderDetail, orderInfo));
            }
        }

        @Override
        public void processElement2(JSONObject orderInfo, Context context, Collector<JSONObject> collector) throws Exception {
            if (isInvalidOrderInfo(orderInfo)) {
                context.output(DIRTY_DATA_TAG, orderInfo.toJSONString());
                return;
            }

            orderInfoState.update(orderInfo);
            JSONObject orderDetail = orderDetailState.value();
            if (orderDetail != null && !isInvalidOrderDetail(orderDetail)) {
                collector.collect(buildBaseFact(orderDetail, orderInfo));
            }
        }
    }

    /**
     * 订单明细事实和活动明细状态关联函数。
     */
    private static class ActivityJoinFunction
            extends KeyedCoProcessFunction<String, JSONObject, JSONObject, JSONObject> {

        private transient ValueState<JSONObject> factState;
        private transient ValueState<JSONObject> activityState;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            factState = getRuntimeContext().getState(createJsonStateDescriptor("activity-fact-state"));
            activityState = getRuntimeContext().getState(createJsonStateDescriptor("activity-state"));
        }

        @Override
        public void processElement1(JSONObject fact, Context context, Collector<JSONObject> collector) throws Exception {
            factState.update(fact);
            JSONObject activity = activityState.value();
            collector.collect(mergeActivity(fact, activity));
        }

        @Override
        public void processElement2(JSONObject activity, Context context, Collector<JSONObject> collector) throws Exception {
            if (isBlank(activity.getString("order_detail_id"))) {
                context.output(DIRTY_DATA_TAG, activity.toJSONString());
                return;
            }

            activityState.update(activity);
            JSONObject fact = factState.value();
            if (fact != null) {
                collector.collect(mergeActivity(fact, activity));
            }
        }
    }

    /**
     * 订单明细事实和优惠券明细状态关联函数。
     */
    private static class CouponJoinFunction
            extends KeyedCoProcessFunction<String, JSONObject, JSONObject, JSONObject> {

        private transient ValueState<JSONObject> factState;
        private transient ValueState<JSONObject> couponState;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            factState = getRuntimeContext().getState(createJsonStateDescriptor("coupon-fact-state"));
            couponState = getRuntimeContext().getState(createJsonStateDescriptor("coupon-state"));
        }

        @Override
        public void processElement1(JSONObject fact, Context context, Collector<JSONObject> collector) throws Exception {
            factState.update(fact);
            JSONObject coupon = couponState.value();
            collector.collect(mergeCoupon(fact, coupon));
        }

        @Override
        public void processElement2(JSONObject coupon, Context context, Collector<JSONObject> collector) throws Exception {
            if (isBlank(coupon.getString("order_detail_id"))) {
                context.output(DIRTY_DATA_TAG, coupon.toJSONString());
                return;
            }

            couponState.update(coupon);
            JSONObject fact = factState.value();
            if (fact != null) {
                collector.collect(mergeCoupon(fact, coupon));
            }
        }
    }

    /**
     * 构造订单明细基础事实，只保留ADS/DWS需要的明细、订单和元字段。
     */
    private static JSONObject buildBaseFact(JSONObject orderDetail, JSONObject orderInfo) {
        JSONObject fact = new JSONObject();

        putString(fact, "id", orderDetail, "id");
        putString(fact, "order_id", orderDetail, "order_id");
        putString(fact, "sku_id", orderDetail, "sku_id");
        putString(fact, "sku_name", orderDetail, "sku_name");
        putString(fact, "order_price", orderDetail, "order_price");
        putString(fact, "sku_num", orderDetail, "sku_num");
        putAmount(fact, "split_total_amount", orderDetail);
        putAmount(fact, "split_activity_amount", orderDetail);
        putAmount(fact, "split_coupon_amount", orderDetail);
        putString(fact, "create_time", orderDetail, "create_time");

        putString(fact, "user_id", orderInfo, "user_id");
        putString(fact, "province_id", orderInfo, "province_id");
        putString(fact, "order_status", orderInfo, "order_status");
        putString(fact, "payment_way", orderInfo, "payment_way");
        putAmount(fact, "original_total_amount", orderInfo);

        fact.put("activity_id", null);
        fact.put("activity_rule_id", null);
        fact.put("coupon_id", null);
        fact.put("coupon_use_id", null);
        fact.put("_fact_type", "order_detail");
        fact.put("_join_ts", System.currentTimeMillis());
        return fact;
    }

    private static JSONObject mergeActivity(JSONObject fact, JSONObject activity) {
        JSONObject result = copyJson(fact);
        if (activity != null) {
            putString(result, "activity_id", activity, "activity_id");
            putString(result, "activity_rule_id", activity, "activity_rule_id");
        }
        result.put("_join_ts", System.currentTimeMillis());
        return result;
    }

    private static JSONObject mergeCoupon(JSONObject fact, JSONObject coupon) {
        JSONObject result = copyJson(fact);
        if (coupon != null) {
            putString(result, "coupon_id", coupon, "coupon_id");
            putString(result, "coupon_use_id", coupon, "coupon_use_id");
        }
        result.put("_join_ts", System.currentTimeMillis());
        return result;
    }

    private static ValueStateDescriptor<JSONObject> createJsonStateDescriptor(String stateName) {
        ValueStateDescriptor<JSONObject> descriptor = new ValueStateDescriptor<JSONObject>(
                stateName,
                JSONObject.class
        );
        descriptor.enableTimeToLive(
                StateTtlConfig.newBuilder(Time.minutes(STATE_TTL_MINUTES))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        .build()
        );
        return descriptor;
    }

    private static FlinkKafkaProducer<String> createKafkaProducer(String topic, Properties properties) {
        return new FlinkKafkaProducer<String>(
                topic,
                new SimpleStringSchema(StandardCharsets.UTF_8),
                properties
        );
    }

    private static boolean isInvalidOrderDetail(JSONObject orderDetail) {
        return isBlank(orderDetail.getString("id"))
                || isBlank(orderDetail.getString("order_id"))
                || isBlank(orderDetail.getString("sku_id"))
                || !isValidInteger(orderDetail.getString("sku_num"))
                || !isValidAmount(orderDetail.getString("order_price"))
                || !isValidAmount(orderDetail.getString("split_total_amount"))
                || !isValidAmount(orderDetail.getString("split_activity_amount"))
                || !isValidAmount(orderDetail.getString("split_coupon_amount"));
    }

    private static boolean isInvalidOrderInfo(JSONObject orderInfo) {
        return isBlank(orderInfo.getString("id"))
                || !isValidAmount(orderInfo.getString("original_total_amount"));
    }

    /**
     * 在keyBy之前先校验关联键，避免空key导致Flink运行时报错。
     */
    private static boolean isInvalidSourceRecord(String sourceName, JSONObject jsonObject) {
        if ("order-detail".equals(sourceName)) {
            return isInvalidOrderDetail(jsonObject);
        }
        if ("order-info".equals(sourceName)) {
            return isInvalidOrderInfo(jsonObject);
        }
        if ("order-detail-activity".equals(sourceName) || "order-detail-coupon".equals(sourceName)) {
            return isBlank(jsonObject.getString("order_detail_id"));
        }
        return false;
    }

    private static void putString(JSONObject target, String targetKey, JSONObject source, String sourceKey) {
        target.put(targetKey, source.getString(sourceKey));
    }

    private static void putAmount(JSONObject target, String key, JSONObject source) {
        String value = source.getString(key);
        target.put(key, isBlank(value) ? BigDecimal.ZERO.toPlainString() : value);
    }

    /**
     * 金额字段允许为空，空值会在输出时补0；非空时必须是合法数字。
     */
    private static boolean isValidAmount(String value) {
        if (isBlank(value)) {
            return true;
        }

        try {
            new BigDecimal(value);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 数量字段必须是合法整数，避免商品件数出现非数字脏值。
     */
    private static boolean isValidInteger(String value) {
        if (isBlank(value)) {
            return false;
        }

        try {
            Long.parseLong(value);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static JSONObject copyJson(JSONObject source) {
        return JSON.parseObject(source.toJSONString());
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().length() == 0;
    }
}
