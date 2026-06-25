CREATE DATABASE IF NOT EXISTS gmall_realtime;

CREATE TABLE IF NOT EXISTS gmall_realtime.dws_traffic_page_view_window (
  stt datetime COMMENT '窗口开始时间',
  edt datetime COMMENT '窗口结束时间',
  cur_date date COMMENT '统计日期',
  pv_ct bigint COMMENT '页面访问次数',
  uv_ct bigint COMMENT '独立访客数',
  sv_ct bigint COMMENT '会话数',
  ts bigint COMMENT '写入时间戳'
)
DUPLICATE KEY(stt, edt)
DISTRIBUTED BY HASH(stt) BUCKETS 10
PROPERTIES (
  "replication_num" = "1"
);

SELECT *
FROM gmall_realtime.dws_traffic_page_view_window
ORDER BY stt DESC
LIMIT 20;
