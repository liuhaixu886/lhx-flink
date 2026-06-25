CREATE DATABASE IF NOT EXISTS gmall_realtime;

-- SKU 交易总览 ADS。
-- 开发环境使用 DWS 表里的最新统计日期，避免模拟数据日期和 Doris 当前日期不一致导致 ADS 查不到数据。
-- sku_order_count 是 SKU 明细行粒度下单次数，不对 order_id 做去重。
DROP VIEW IF EXISTS gmall_realtime.ads_trade_stats_today;
CREATE VIEW gmall_realtime.ads_trade_stats_today AS
SELECT
  cur_date,
  SUM(order_amount) AS gmv,
  SUM(order_count) AS sku_order_count,
  SUM(sku_num) AS sku_num,
  COUNT(DISTINCT sku_id) AS ordered_sku_count,
  MAX(edt) AS latest_window_time,
  UNIX_TIMESTAMP() * 1000 AS ts
FROM gmall_realtime.dws_trade_sku_order_window
WHERE cur_date = (
  SELECT MAX(cur_date)
  FROM gmall_realtime.dws_trade_sku_order_window
)
GROUP BY cur_date;

-- SKU 下单金额 TopN ADS。
DROP VIEW IF EXISTS gmall_realtime.ads_trade_sku_order_topn_today;
CREATE VIEW gmall_realtime.ads_trade_sku_order_topn_today AS
SELECT
  cur_date,
  sku_id,
  sku_name,
  order_amount,
  sku_order_count,
  sku_num,
  ROW_NUMBER() OVER (PARTITION BY cur_date ORDER BY order_amount DESC) AS rn,
  UNIX_TIMESTAMP() * 1000 AS ts
FROM (
  SELECT
    cur_date,
    sku_id,
    MAX(sku_name) AS sku_name,
    SUM(order_amount) AS order_amount,
    SUM(order_count) AS sku_order_count,
    SUM(sku_num) AS sku_num
  FROM gmall_realtime.dws_trade_sku_order_window
  WHERE cur_date = (
    SELECT MAX(cur_date)
    FROM gmall_realtime.dws_trade_sku_order_window
  )
  GROUP BY cur_date, sku_id
) sku_stats;

-- 30 秒交易趋势 ADS。
DROP VIEW IF EXISTS gmall_realtime.ads_trade_trend_30s;
CREATE VIEW gmall_realtime.ads_trade_trend_30s AS
SELECT
  stt,
  edt,
  cur_date,
  SUM(order_amount) AS gmv,
  SUM(order_count) AS sku_order_count,
  SUM(sku_num) AS sku_num,
  COUNT(DISTINCT sku_id) AS ordered_sku_count,
  UNIX_TIMESTAMP() * 1000 AS ts
FROM gmall_realtime.dws_trade_sku_order_window
WHERE cur_date = (
  SELECT MAX(cur_date)
  FROM gmall_realtime.dws_trade_sku_order_window
)
GROUP BY stt, edt, cur_date;

-- 小时交易趋势 ADS。
DROP VIEW IF EXISTS gmall_realtime.ads_trade_hour_stats_today;
CREATE VIEW gmall_realtime.ads_trade_hour_stats_today AS
SELECT
  cur_date,
  DATE_FORMAT(stt, '%H:00:00') AS hour_time,
  SUM(order_amount) AS gmv,
  SUM(order_count) AS sku_order_count,
  SUM(sku_num) AS sku_num,
  COUNT(DISTINCT sku_id) AS ordered_sku_count,
  UNIX_TIMESTAMP() * 1000 AS ts
FROM gmall_realtime.dws_trade_sku_order_window
WHERE cur_date = (
  SELECT MAX(cur_date)
  FROM gmall_realtime.dws_trade_sku_order_window
)
GROUP BY cur_date, DATE_FORMAT(stt, '%H:00:00');

SELECT *
FROM gmall_realtime.ads_trade_stats_today;

SELECT *
FROM gmall_realtime.ads_trade_sku_order_topn_today
WHERE rn <= 10
ORDER BY rn;

SELECT *
FROM gmall_realtime.ads_trade_trend_30s
ORDER BY stt DESC
LIMIT 20;

SELECT *
FROM gmall_realtime.ads_trade_hour_stats_today
ORDER BY hour_time;
