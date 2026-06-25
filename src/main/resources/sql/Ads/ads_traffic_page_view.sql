CREATE DATABASE IF NOT EXISTS gmall_realtime;

-- 流量总览 ADS。
-- 开发环境使用 DWS 表里的最新统计日期，避免模拟数据日期和 Doris 当前日期不一致导致 ADS 查不到数据。
DROP VIEW IF EXISTS gmall_realtime.ads_traffic_stats_today;
CREATE VIEW gmall_realtime.ads_traffic_stats_today AS
SELECT
  cur_date,
  SUM(pv_ct) AS pv_ct,
  SUM(uv_ct) AS uv_ct,
  SUM(sv_ct) AS sv_ct,
  MAX(edt) AS latest_window_time,
  UNIX_TIMESTAMP() * 1000 AS ts
FROM gmall_realtime.dws_traffic_page_view_window
WHERE cur_date = (
  SELECT MAX(cur_date)
  FROM gmall_realtime.dws_traffic_page_view_window
)
GROUP BY cur_date;

-- 30 秒流量趋势 ADS。
DROP VIEW IF EXISTS gmall_realtime.ads_traffic_page_view_trend_30s;
CREATE VIEW gmall_realtime.ads_traffic_page_view_trend_30s AS
SELECT
  stt,
  edt,
  cur_date,
  SUM(pv_ct) AS pv_ct,
  SUM(uv_ct) AS uv_ct,
  SUM(sv_ct) AS sv_ct,
  UNIX_TIMESTAMP() * 1000 AS ts
FROM gmall_realtime.dws_traffic_page_view_window
WHERE cur_date = (
  SELECT MAX(cur_date)
  FROM gmall_realtime.dws_traffic_page_view_window
)
GROUP BY stt, edt, cur_date;

-- 小时流量趋势 ADS。
DROP VIEW IF EXISTS gmall_realtime.ads_traffic_hour_stats_today;
CREATE VIEW gmall_realtime.ads_traffic_hour_stats_today AS
SELECT
  cur_date,
  DATE_FORMAT(stt, '%H:00:00') AS hour_time,
  SUM(pv_ct) AS pv_ct,
  SUM(uv_ct) AS uv_ct,
  SUM(sv_ct) AS sv_ct,
  UNIX_TIMESTAMP() * 1000 AS ts
FROM gmall_realtime.dws_traffic_page_view_window
WHERE cur_date = (
  SELECT MAX(cur_date)
  FROM gmall_realtime.dws_traffic_page_view_window
)
GROUP BY cur_date, DATE_FORMAT(stt, '%H:00:00');

SELECT *
FROM gmall_realtime.ads_traffic_stats_today;

SELECT *
FROM gmall_realtime.ads_traffic_page_view_trend_30s
ORDER BY stt DESC
LIMIT 20;

SELECT *
FROM gmall_realtime.ads_traffic_hour_stats_today
ORDER BY hour_time;
