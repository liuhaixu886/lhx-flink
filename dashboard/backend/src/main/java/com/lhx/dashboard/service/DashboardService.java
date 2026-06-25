package com.lhx.dashboard.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ADS 看板查询服务。
 * 所有 SQL 固定查询 Doris ADS 视图，前端不能传入任意 SQL。
 */
@Service
public class DashboardService {

    private static final int DEFAULT_TOPN_LIMIT = 10;
    private static final int MAX_TOPN_LIMIT = 50;

    private final JdbcTemplate jdbcTemplate;

    public DashboardService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * 查询交易和流量核心总览指标。
     */
    public Map<String, Object> querySummary() {
        Map<String, Object> trade = queryOne(
                "SELECT cur_date, gmv, sku_order_count, sku_num, ordered_sku_count, latest_window_time " +
                        "FROM ads_trade_stats_today LIMIT 1"
        );
        Map<String, Object> traffic = queryOne(
                "SELECT cur_date, pv_ct, uv_ct, sv_ct, latest_window_time " +
                        "FROM ads_traffic_stats_today LIMIT 1"
        );

        Map<String, Object> summary = new HashMap<String, Object>();
        summary.put("trade", trade);
        summary.put("traffic", traffic);
        summary.put("curDate", firstNotBlank(trade.get("cur_date"), traffic.get("cur_date")));
        summary.put("latestWindowTime", maxString(trade.get("latest_window_time"), traffic.get("latest_window_time")));
        return summary;
    }

    /**
     * 查询 30 秒交易趋势。
     */
    public List<Map<String, Object>> queryTradeTrend() {
        return queryList(
                "SELECT stt, edt, cur_date, gmv, sku_order_count, sku_num, ordered_sku_count " +
                        "FROM ads_trade_trend_30s ORDER BY stt"
        );
    }

    /**
     * 查询 30 秒流量趋势。
     */
    public List<Map<String, Object>> queryTrafficTrend() {
        return queryList(
                "SELECT stt, edt, cur_date, pv_ct, uv_ct, sv_ct " +
                        "FROM ads_traffic_page_view_trend_30s ORDER BY stt"
        );
    }

    /**
     * 查询 SKU 下单金额 TopN。
     */
    public List<Map<String, Object>> querySkuTopN(Integer limit) {
        int safeLimit = normalizeLimit(limit);
        return queryList(
                "SELECT cur_date, sku_id, sku_name, order_amount, sku_order_count, sku_num, rn " +
                        "FROM ads_trade_sku_order_topn_today WHERE rn <= ? ORDER BY rn",
                safeLimit
        );
    }

    /**
     * 查询省份交易金额 TopN。
     */
    public List<Map<String, Object>> queryProvinceTopN(Integer limit) {
        int safeLimit = normalizeLimit(limit);
        return queryList(
                "SELECT cur_date, province_id, gmv, province_order_count, sku_num, rn " +
                        "FROM ads_trade_province_order_topn_today WHERE rn <= ? ORDER BY rn",
                safeLimit
        );
    }

    /**
     * 查询小时级交易和流量趋势。
     */
    public Map<String, Object> queryHourTrend() {
        Map<String, Object> result = new HashMap<String, Object>();
        result.put("trade", queryList(
                "SELECT cur_date, hour_time, gmv, sku_order_count, sku_num, ordered_sku_count " +
                        "FROM ads_trade_hour_stats_today ORDER BY hour_time"
        ));
        result.put("traffic", queryList(
                "SELECT cur_date, hour_time, pv_ct, uv_ct, sv_ct " +
                        "FROM ads_traffic_hour_stats_today ORDER BY hour_time"
        ));
        return result;
    }

    private Map<String, Object> queryOne(String sql) {
        List<Map<String, Object>> rows = queryList(sql);
        if (rows.isEmpty()) {
            return new HashMap<String, Object>();
        }
        return rows.get(0);
    }

    private List<Map<String, Object>> queryList(String sql, Object... args) {
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, args);
        for (Map<String, Object> row : rows) {
            normalizeRow(row);
        }
        return rows;
    }

    private int normalizeLimit(Integer limit) {
        if (limit == null || limit <= 0) {
            return DEFAULT_TOPN_LIMIT;
        }
        return Math.min(limit, MAX_TOPN_LIMIT);
    }

    private void normalizeRow(Map<String, Object> row) {
        SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Timestamp) {
                entry.setValue(dateTimeFormat.format((Timestamp) value));
            } else if (value instanceof Date) {
                entry.setValue(dateFormat.format((Date) value));
            }
        }
    }

    private Object firstNotBlank(Object first, Object second) {
        if (first != null && first.toString().trim().length() > 0) {
            return first;
        }
        return second;
    }

    private Object maxString(Object first, Object second) {
        if (first == null) {
            return second;
        }
        if (second == null) {
            return first;
        }
        return first.toString().compareTo(second.toString()) >= 0 ? first : second;
    }
}
