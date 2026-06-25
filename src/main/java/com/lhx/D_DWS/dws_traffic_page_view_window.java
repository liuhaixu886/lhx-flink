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
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * DWS 流量域页面访问窗口汇总作业。
 * 消费 DWD 页面日志，按 30 秒滚动窗口统计 PV、UV、SV，写入 Doris 支撑 ADS 流量看板。
 */
public class dws_traffic_page_view_window {

    /** Kafka 集群地址。 */
    private static final String KAFKA_BOOTSTRAP_SERVERS = "node101:9092,node102:9092,node103:9092";

    /** DWD 页面日志主题。 */
    private static final String SOURCE_TOPIC_PAGE_LOG = "dwd_traffic_page_log";

    /** 统一脏数据主题。 */
    private static final String SINK_TOPIC_DIRTY_DATA = "dirty_data";

    /** 当前作业消费者组。 */
    private static final String CONSUMER_GROUP_ID = "dws_traffic_page_view_window";

    /** Doris 写入配置，FE 使用 HTTP 端口 8030。 */
    private static final String DORIS_FE_NODES = "node102:8030";
    private static final String DORIS_TABLE_IDENTIFIER = "gmall_realtime.dws_traffic_page_view_window";
    private static final String DORIS_USERNAME = "root";
    private static final String DORIS_PASSWORD = "123456";

    /** 侧输出流标签：脏数据。 */
    private static final OutputTag<String> DIRTY_DATA_TAG = new OutputTag<String>("dirty-data") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Checkpoint 用于保存 Kafka 消费位点和窗口状态，异常重启后可继续处理。
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
                SOURCE_TOPIC_PAGE_LOG,
                new SimpleStringSchema(StandardCharsets.UTF_8),
                kafkaConsumerProperties
        );
        kafkaConsumer.setStartFromGroupOffsets();

        DataStreamSource<String> sourceStream = env.addSource(kafkaConsumer, "kafka-dwd-page-log-source");

        SingleOutputStreamOperator<PageViewEvent> pageViewStream = sourceStream.process(
                new ProcessFunction<String, PageViewEvent>() {
                    @Override
                    public void processElement(String value, Context context, Collector<PageViewEvent> collector) {
                        try {
                            collector.collect(parsePageView(value));
                        } catch (Exception e) {
                            context.output(DIRTY_DATA_TAG, value);
                        }
                    }
                }
        ).name("parse-and-clean-page-view-log");

        SingleOutputStreamOperator<String> windowResultStream = pageViewStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<PageViewEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(new SerializableTimestampAssigner<PageViewEvent>() {
                                    @Override
                                    public long extractTimestamp(PageViewEvent element, long recordTimestamp) {
                                        return element.eventTime;
                                    }
                                })
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(30)))
                .process(new PageViewWindowFunction())
                .name("page-view-30-second-window");

        DataStream<String> dirtyDataStream = pageViewStream.getSideOutput(DIRTY_DATA_TAG);

        Properties kafkaProducerProperties = new Properties();
        kafkaProducerProperties.setProperty("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
        dirtyDataStream.addSink(
                new FlinkKafkaProducer<String>(
                        SINK_TOPIC_DIRTY_DATA,
                        new SimpleStringSchema(StandardCharsets.UTF_8),
                        kafkaProducerProperties
                )
        ).name("kafka-dirty-data-sink");

        // 打印窗口结果，便于本地观察 PV、UV、SV 是否正常产出。
        windowResultStream.print("dws-traffic-page-view-window");

        windowResultStream.addSink(createDorisSink()).name("doris-dws-traffic-page-view-window-sink");

        env.execute("dws_traffic_page_view_window");
    }

    /**
     * 解析页面访问日志，提取 mid、sid、ts。mid 用于 UV，sid 用于 SV。
     */
    private static PageViewEvent parsePageView(String value) {
        if (isBlank(value)) {
            throw new IllegalArgumentException("empty value");
        }

        JSONObject root = JSON.parseObject(value);
        JSONObject common = root.getJSONObject("common");
        JSONObject page = root.getJSONObject("page");
        Long ts = root.getLong("ts");
        if (common == null || page == null || ts == null) {
            throw new IllegalArgumentException("missing common/page/ts");
        }

        String mid = common.getString("mid");
        if (isBlank(mid)) {
            throw new IllegalArgumentException("missing common.mid");
        }

        PageViewEvent event = new PageViewEvent();
        event.mid = mid;
        event.sid = common.getString("sid");
        event.eventTime = ts;
        return event;
    }

    /**
     * 创建 Doris Sink。使用 TSV 文本格式，避免 Doris Connector 1.0.3 的 String JSON 批量格式问题。
     */
    private static org.apache.flink.streaming.api.functions.sink.SinkFunction<String> createDorisSink() {
        Properties streamLoadProperties = new Properties();
        streamLoadProperties.setProperty("format", "csv");
        streamLoadProperties.setProperty("column_separator", "\t");
        streamLoadProperties.setProperty("line_delimiter", "\n");
        streamLoadProperties.setProperty("columns", "stt,edt,cur_date,pv_ct,uv_ct,sv_ct,ts");

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
     * 30 秒页面访问窗口聚合。PV 直接计数，UV 按 mid 去重，SV 按 sid 去重。
     */
    private static class PageViewWindowFunction
            extends ProcessAllWindowFunction<PageViewEvent, String, TimeWindow> {

        private final SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        @Override
        public void process(Context context, Iterable<PageViewEvent> elements, Collector<String> collector) {
            long pv = 0L;
            Set<String> midSet = new HashSet<String>();
            Set<String> sidSet = new HashSet<String>();

            for (PageViewEvent event : elements) {
                pv++;
                midSet.add(event.mid);
                if (!isBlank(event.sid)) {
                    sidSet.add(event.sid);
                }
            }

            long windowStart = context.window().getStart();
            long windowEnd = context.window().getEnd();

            collector.collect(buildDorisCsvLine(
                    dateTimeFormat.format(new Date(windowStart)),
                    dateTimeFormat.format(new Date(windowEnd)),
                    dateFormat.format(new Date(windowStart)),
                    String.valueOf(pv),
                    String.valueOf(midSet.size()),
                    String.valueOf(sidSet.size()),
                    String.valueOf(System.currentTimeMillis())
            ));
        }
    }

    /**
     * 页面访问 DWS 使用的轻量对象。
     */
    public static class PageViewEvent {
        public String mid;
        public String sid;
        public long eventTime;
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
