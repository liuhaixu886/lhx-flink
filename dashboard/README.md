# ADS 实时可视化看板

## 说明

看板只查询 Doris ADS 视图，不直接访问 ODS、DWD、DWS。页面每 5 秒轮询后端接口，底层指标按当前 DWS/ADS 的 30 秒窗口更新。

## 依赖

- Doris MySQL 查询端口：`node102:9030`
- Doris 库：`gmall_realtime`
- 用户：`root`
- 密码：`123456`
- 后端端口：`8088`
- 前端端口：`5173`

## 后端启动

```bash
cd dashboard/backend
mvn spring-boot:run
```

接口：

- `GET /api/dashboard/summary`
- `GET /api/dashboard/trade-trend`
- `GET /api/dashboard/traffic-trend`
- `GET /api/dashboard/sku-topn?limit=10`
- `GET /api/dashboard/province-topn?limit=10`
- `GET /api/dashboard/hour-trend`

## 前端启动

```bash
cd dashboard/frontend
npm install
npm run dev
```

访问：

```text
http://localhost:5173
```

## 数据来源

- `ads_trade_stats_today`
- `ads_trade_sku_order_topn_today`
- `ads_trade_trend_30s`
- `ads_trade_hour_stats_today`
- `ads_trade_province_order_today`
- `ads_trade_province_order_topn_today`
- `ads_traffic_stats_today`
- `ads_traffic_page_view_trend_30s`
- `ads_traffic_hour_stats_today`

## 验证 SQL

```sql
SELECT * FROM gmall_realtime.ads_trade_stats_today;
SELECT * FROM gmall_realtime.ads_trade_sku_order_topn_today WHERE rn <= 10 ORDER BY rn;
SELECT * FROM gmall_realtime.ads_trade_province_order_topn_today WHERE rn <= 10 ORDER BY rn;
SELECT * FROM gmall_realtime.ads_traffic_stats_today;
SELECT * FROM gmall_realtime.ads_traffic_page_view_trend_30s ORDER BY stt DESC LIMIT 20;
```
