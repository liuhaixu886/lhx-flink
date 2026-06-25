package com.lhx.D_DWS;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisSink;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Properties;

/**
 * DWS 交易域省份下单窗口汇总作业。
 * 消费订单明细事实宽流，按省份做 30 秒滚动窗口聚合，写入 Doris 支撑 ADS 省份交易排行和地区大屏。
 */
public class dws_trade_province_order_window {

    /** Kafka 集群地址。 */
    private static final String KAFKA_BOOTSTRAP_SERVERS = "node101:9092,node102:9092,node103:9092";

    /** DWD 订单明细事实宽流主题。 */
    private static final String SOURCE_TOPIC_ORDER_DETAIL_FACT = "dwd_trade_order_detail_fact";

    /** 统一脏数据主题。 */
    private static final String SINK_TOPIC_DIRTY_DATA = "dirty_data";

    /** 当前作业消费者组。 */
    private static final String CONSUMER_GROUP_ID = "dws_trade_province_order_window";

    /** Doris 写入配置，FE 使用 HTTP 端口 8030。 */
    private static final String DORIS_FE_NODES = "node102:8030";
    private static final String DORIS_TABLE_IDENTIFIER = "gmall_realtime.dws_trade_province_order_window";
    private static final String DORIS_USERNAME = "root";
    private static final String DORIS_PASSWORD = "123456";

    /** 侧输出流标签：脏数据。 */
    private static final OutputTag<String> DIRTY_DATA_TAG = new OutputTag<String>("dirty-data") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Checkpoint 用于提交 Kafka 消费位点和恢复窗口状态。
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
                SOURCE_TOPIC_ORDER_DETAIL_FACT,
                new SimpleStringSchema(StandardCharsets.UTF_8),
                kafkaConsumerProperties
        );
        kafkaConsumer.setStartFromGroupOffsets();

        DataStreamSource<String> sourceStream = env.addSource(kafkaConsumer, "kafka-dwd-order-detail-fact-source");

        SingleOutputStreamOperator<ProvinceOrderFact> factStream = sourceStream.process(
                new ProcessFunction<String, ProvinceOrderFact>() {
                    @Override
                    public void processElement(String value, Context context, Collector<ProvinceOrderFact> collector) {
                        try {
                            collector.collect(parseFact(value));
                        } catch (Exception e) {
                            context.output(DIRTY_DATA_TAG, value);
                        }
                    }
                }
        ).name("parse-and-clean-province-order-fact");

        SingleOutputStreamOperator<String> windowResultStream = factStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<ProvinceOrderFact>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(new SerializableTimestampAssigner<ProvinceOrderFact>() {
                                    @Override
                                    public long extractTimestamp(ProvinceOrderFact element, long recordTimestamp) {
                                        return element.eventTime;
                                    }
                                })
                )
                .keyBy(ProvinceOrderFact::getProvinceId)
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .process(new ProvinceOrderWindowFunction())
                .name("province-order-30-second-window");

        DataStream<String> dirtyDataStream = factStream.getSideOutput(DIRTY_DATA_TAG);

        Properties kafkaProducerProperties = new Properties();
        kafkaProducerProperties.setProperty("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
        dirtyDataStream.addSink(
                new FlinkKafkaProducer<String>(
                        SINK_TOPIC_DIRTY_DATA,
                        new SimpleStringSchema(StandardCharsets.UTF_8),
                        kafkaProducerProperties
                )
        ).name("kafka-dirty-data-sink");

        // 打印窗口结果，便于本地先确认省份 DWS 是否产生汇总数据。
        windowResultStream.print("dws-province-order-window");

        windowResultStream.addSink(createDorisSink()).name("doris-dws-province-order-window-sink");

        env.execute("dws_trade_province_order_window");
    }

    /**
     * 解析订单明细事实宽流，并校验省份、金额、件数字段。
     */
    private static ProvinceOrderFact parseFact(String value) {
        if (isBlank(value)) {
            throw new IllegalArgumentException("empty value");
        }

        JSONObject jsonObject = JSON.parseObject(value);
        String provinceId = jsonObject.getString("province_id");
        if (isBlank(provinceId)) {
            throw new IllegalArgumentException("missing province_id");
        }

        ProvinceOrderFact fact = new ProvinceOrderFact();
        fact.provinceId = provinceId;
        fact.orderAmount = parseAmount(jsonObject.getString("split_total_amount"));
        fact.skuNum = parseLong(jsonObject.getString("sku_num"));
        // 交易看板需要观察实时造数效果，GMall 业务表里的 create_time 常是模拟业务时间。
        // 如果继续用 create_time 做事件时间，后续新造的数据可能落到已经关闭的历史窗口里，导致看板不刷新。
        // 这里用处理时间入 30 秒窗口，保证业务数据一进入 DWS 就能推动实时看板变化。
        fact.eventTime = System.currentTimeMillis();
        return fact;
    }

    /**
     * 创建 Doris Sink。这里使用 TSV 文本格式，避免 Doris Connector 1.0.3 的 String JSON 批量格式问题。
     */
    private static org.apache.flink.streaming.api.functions.sink.SinkFunction<String> createDorisSink() {
        Properties streamLoadProperties = new Properties();
        streamLoadProperties.setProperty("format", "csv");
        streamLoadProperties.setProperty("column_separator", "\t");
        streamLoadProperties.setProperty("line_delimiter", "\n");
        streamLoadProperties.setProperty("columns", "province_id,stt,edt,cur_date,order_amount,order_count,sku_num,ts");

        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes(DORIS_FE_NODES)
                .setTableIdentifier(DORIS_TABLE_IDENTIFIER)
                .setUsername(DORIS_USERNAME)
                .setPassword(DORIS_PASSWORD)
                .build();

        DorisExecutionOptions executionOptions = DorisExecutionOptions.builder()
                .setBatchSize(1000)
                .setBatchIntervalMs(5000L)
                .setMaxRetries(3)
                .setStreamLoadProp(streamLoadProperties)
                .build();

        return DorisSink.sink(
                DorisReadOptions.builder().build(),
                executionOptions,
                dorisOptions
        );
    }

    /**
     * 省份维度窗口聚合函数。order_count 按明细行数统计，不对 order_id 去重。
     */
    private static class ProvinceOrderWindowFunction
            extends ProcessWindowFunction<ProvinceOrderFact, String, String, TimeWindow> {

        private final SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        @Override
        public void process(String provinceId, Context context, Iterable<ProvinceOrderFact> elements, Collector<String> collector) {
            BigDecimal orderAmount = BigDecimal.ZERO;
            long orderCount = 0L;
            long skuNum = 0L;

            for (ProvinceOrderFact element : elements) {
                orderAmount = orderAmount.add(element.orderAmount);
                orderCount++;
                skuNum += element.skuNum;
            }

            long windowStart = context.window().getStart();
            long windowEnd = context.window().getEnd();

            collector.collect(buildDorisCsvLine(
                    provinceId,
                    dateTimeFormat.format(new Date(windowStart)),
                    dateTimeFormat.format(new Date(windowEnd)),
                    dateFormat.format(new Date(windowStart)),
                    orderAmount.setScale(2, BigDecimal.ROUND_HALF_UP).toPlainString(),
                    String.valueOf(orderCount),
                    String.valueOf(skuNum),
                    String.valueOf(System.currentTimeMillis())
            ));
        }
    }

    /**
     * DWS 省份交易汇总使用的轻量对象。
     */
    public static class ProvinceOrderFact {
        public String provinceId;
        public BigDecimal orderAmount;
        public long skuNum;
        public long eventTime;

        public String getProvinceId() {
            return provinceId;
        }
    }

    private static BigDecimal parseAmount(String value) {
        if (isBlank(value)) {
            return BigDecimal.ZERO;
        }
        return new BigDecimal(value);
    }

    private static long parseLong(String value) {
        if (isBlank(value)) {
            throw new IllegalArgumentException("empty long value");
        }
        return Long.parseLong(value);
    }

    private static long parseEventTime(String value) {
        if (isBlank(value)) {
            return System.currentTimeMillis();
        }

        try {
            return Long.parseLong(value);
        } catch (Exception ignored) {
            try {
                return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(value).getTime();
            } catch (Exception e) {
                return System.currentTimeMillis();
            }
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().length() == 0;
    }

    /**
     * 构造写入 Doris 的 TSV 行，字段顺序必须和 Stream Load columns 保持一致。
     */
    private static String buildDorisCsvLine(String... fields) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) {
                builder.append('\t');
            }
            builder.append(cleanDorisCsvField(fields[i]));
        }
        return builder.toString();
    }

    /**
     * 清理字段中的制表符和换行符，避免破坏 Doris CSV 分隔格式。
     */
    private static String cleanDorisCsvField(String value) {
        if (value == null) {
            return "";
        }
        return value.replace('\t', ' ')
                .replace('\r', ' ')
                .replace('\n', ' ');
    }
}
