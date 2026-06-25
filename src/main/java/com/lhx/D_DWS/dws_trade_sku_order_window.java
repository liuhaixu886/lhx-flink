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
 * DWS交易域SKU下单窗口汇总作业。
 * 消费订单明细事实宽流，按SKU做30秒滚动窗口聚合，并写入Doris供ADS实时大屏使用。
 *
 * @author 刘海旭
 * @date 2026/05/26
 */
public class dws_trade_sku_order_window {

    /** Kafka集群地址。 */
    private static final String KAFKA_BOOTSTRAP_SERVERS = "node101:9092,node102:9092,node103:9092";

    /** DWD订单明细事实输入主题。 */
    private static final String SOURCE_TOPIC_ORDER_DETAIL_FACT = "dwd_trade_order_detail_fact";

    /** 统一脏数据主题。 */
    private static final String SINK_TOPIC_DIRTY_DATA = "dirty_data";

    /** 当前作业的Kafka消费者组。 */
    private static final String CONSUMER_GROUP_ID = "dws_trade_sku_order_window";

    /** Doris连接配置。 */
    private static final String DORIS_FE_NODES = "node102:8030";
    private static final String DORIS_TABLE_IDENTIFIER = "gmall_realtime.dws_trade_sku_order_window";
    private static final String DORIS_USERNAME = "root";
    private static final String DORIS_PASSWORD = "123456";

    /** 侧输出流标签：脏数据。 */
    private static final OutputTag<String> DIRTY_DATA_TAG = new OutputTag<String>("dirty-data") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Checkpoint用于提交Kafka消费位点和Doris Stream Load，失败后可以从一致状态恢复。
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

        SingleOutputStreamOperator<SkuOrderFact> factStream = sourceStream.process(
                new ProcessFunction<String, SkuOrderFact>() {
                    @Override
                    public void processElement(String value, Context context, Collector<SkuOrderFact> collector) {
                        try {
                            SkuOrderFact fact = parseFact(value);
                            collector.collect(fact);
                        } catch (Exception e) {
                            context.output(DIRTY_DATA_TAG, value);
                        }
                    }
                }
        ).name("parse-and-clean-order-detail-fact");

        SingleOutputStreamOperator<String> windowResultStream = factStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<SkuOrderFact>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(new SerializableTimestampAssigner<SkuOrderFact>() {
                                    @Override
                                    public long extractTimestamp(SkuOrderFact element, long recordTimestamp) {
                                        return element.eventTime;
                                    }
                                })
                )
                .keyBy(SkuOrderFact::getSkuId)
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .process(new SkuOrderWindowFunction())
                .name("sku-order-30-second-window");

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

        // 打印窗口聚合结果，便于本地调试时先确认 DWS 是否已经产生可写入 Doris 的汇总数据。
        windowResultStream.print("dws-sku-order-window");

        windowResultStream.addSink(createDorisSink()).name("doris-dws-sku-order-window-sink");

        env.execute("dws_trade_sku_order_window");
    }

    /**
     * 解析订单明细事实并做DWS必要字段校验，非法数据抛异常后写入dirty_data。
     */
    private static SkuOrderFact parseFact(String value) {
        if (isBlank(value)) {
            throw new IllegalArgumentException("empty value");
        }

        JSONObject jsonObject = JSON.parseObject(value);
        String skuId = jsonObject.getString("sku_id");
        String orderId = jsonObject.getString("order_id");
        if (isBlank(skuId) || isBlank(orderId)) {
            throw new IllegalArgumentException("missing sku_id or order_id");
        }

        SkuOrderFact fact = new SkuOrderFact();
        fact.skuId = skuId;
        fact.skuName = jsonObject.getString("sku_name");
        fact.orderId = orderId;
        fact.orderAmount = parseAmount(jsonObject.getString("split_total_amount"));
        fact.skuNum = parseLong(jsonObject.getString("sku_num"));
        // 交易看板需要观察实时造数效果，GMall 业务表里的 create_time 常是模拟业务时间。
        // 如果继续用 create_time 做事件时间，后续新造的数据可能落到已经关闭的历史窗口里，导致看板不刷新。
        // 这里用处理时间入 30 秒窗口，保证业务数据一进入 DWS 就能推动实时看板变化。
        fact.eventTime = System.currentTimeMillis();
        return fact;
    }

    /**
     * 创建Doris Sink，使用JSON Stream Load写入Doris明细汇总表。
     */
    private static org.apache.flink.streaming.api.functions.sink.SinkFunction<String> createDorisSink() {
        Properties streamLoadProperties = new Properties();
        // Doris Connector 1.0.3 的 DataStream String Sink 在 JSON 模式下会把批次转成 List 字符串。
        // 这里改用 TSV 文本格式，并显式指定列顺序，避免 Stream Load 把整批 JSON 当成一条坏数据过滤。
        streamLoadProperties.setProperty("format", "csv");
        streamLoadProperties.setProperty("column_separator", "\t");
        streamLoadProperties.setProperty("line_delimiter", "\n");
        streamLoadProperties.setProperty("columns", "sku_id,stt,edt,sku_name,cur_date,order_amount,order_count,sku_num,ts");

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
     * SKU维度窗口聚合函数，窗口内去重统计订单数，并聚合金额和件数。
     */
    private static class SkuOrderWindowFunction extends ProcessWindowFunction<SkuOrderFact, String, String, TimeWindow> {

        private final SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        @Override
        public void process(String skuId, Context context, Iterable<SkuOrderFact> elements, Collector<String> collector) {
            BigDecimal orderAmount = BigDecimal.ZERO;
            long skuNum = 0L;
            String skuName = null;
            long orderCount = 0L;

            for (SkuOrderFact element : elements) {
                orderAmount = orderAmount.add(element.orderAmount);
                skuNum += element.skuNum;
                orderCount++;
                if (isBlank(skuName) && !isBlank(element.skuName)) {
                    skuName = element.skuName;
                }
            }

            long windowStart = context.window().getStart();
            long windowEnd = context.window().getEnd();

            String stt = dateTimeFormat.format(new Date(windowStart));
            String edt = dateTimeFormat.format(new Date(windowEnd));
            String curDate = dateFormat.format(new Date(windowStart));
            String amount = orderAmount.setScale(2, BigDecimal.ROUND_HALF_UP).toPlainString();

            collector.collect(buildDorisCsvLine(
                    skuId,
                    stt,
                    edt,
                    skuName,
                    curDate,
                    amount,
                    String.valueOf(orderCount),
                    String.valueOf(skuNum),
                    String.valueOf(System.currentTimeMillis())
            ));
        }
    }

    /**
     * DWD订单明细事实在DWS内使用的轻量对象。
     */
    public static class SkuOrderFact {
        public String skuId;
        public String skuName;
        public String orderId;
        public BigDecimal orderAmount;
        public long skuNum;
        public long eventTime;

        public String getSkuId() {
            return skuId;
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
     * 构造写入 Doris 的 TSV 行，字段顺序必须和 stream load 的 columns 配置保持一致。
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
