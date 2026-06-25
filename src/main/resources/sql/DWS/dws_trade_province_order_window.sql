CREATE DATABASE IF NOT EXISTS gmall_realtime;

CREATE TABLE IF NOT EXISTS gmall_realtime.dws_trade_province_order_window (
  province_id varchar(50) COMMENT '省份ID',
  stt datetime COMMENT '窗口开始时间',
  edt datetime COMMENT '窗口结束时间',
  cur_date date COMMENT '统计日期',
  order_amount decimal(16,2) COMMENT '下单金额',
  order_count bigint COMMENT '下单次数，按订单明细行统计',
  sku_num bigint COMMENT '下单件数',
  ts bigint COMMENT '写入时间戳'
)
DUPLICATE KEY(province_id, stt, edt)
DISTRIBUTED BY HASH(province_id) BUCKETS 10
PROPERTIES (
  "replication_num" = "1"
);

SELECT *
FROM gmall_realtime.dws_trade_province_order_window
ORDER BY stt DESC
LIMIT 20;
