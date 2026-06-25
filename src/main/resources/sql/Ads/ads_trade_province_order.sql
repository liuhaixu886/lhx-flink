CREATE DATABASE IF NOT EXISTS gmall_realtime;

-- 省份交易汇总 ADS。
-- 开发环境使用 DWS 表里的最新统计日期，避免模拟数据日期和 Doris 当前日期不一致导致 ADS 查不到数据。
DROP VIEW IF EXISTS gmall_realtime.ads_trade_province_order_today;
CREATE VIEW gmall_realtime.ads_trade_province_order_today AS
SELECT
  cur_date,
  province_id,
  SUM(order_amount) AS gmv,
  SUM(order_count) AS province_order_count,
  SUM(sku_num) AS sku_num,
  MAX(edt) AS latest_window_time,
  UNIX_TIMESTAMP() * 1000 AS ts
FROM gmall_realtime.dws_trade_province_order_window
WHERE cur_date = (
  SELECT MAX(cur_date)
  FROM gmall_realtime.dws_trade_province_order_window
)
GROUP BY cur_date, province_id;

-- 省份交易金额 TopN ADS。
DROP VIEW IF EXISTS gmall_realtime.ads_trade_province_order_topn_today;
CREATE VIEW gmall_realtime.ads_trade_province_order_topn_today AS
SELECT
  cur_date,
  province_id,
  gmv,
  province_order_count,
  sku_num,
  ROW_NUMBER() OVER (PARTITION BY cur_date ORDER BY gmv DESC) AS rn,
  UNIX_TIMESTAMP() * 1000 AS ts
FROM gmall_realtime.ads_trade_province_order_today;

SELECT *
FROM gmall_realtime.ads_trade_province_order_today
ORDER BY gmv DESC;

SELECT *
FROM gmall_realtime.ads_trade_province_order_topn_today
WHERE rn <= 10
ORDER BY rn;
