package com.lhx.dashboard.controller;

import com.lhx.dashboard.common.ApiResult;
import com.lhx.dashboard.service.DashboardService;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * ADS 实时看板接口。
 */
@RestController
@CrossOrigin
public class DashboardController {

    private final DashboardService dashboardService;

    public DashboardController(DashboardService dashboardService) {
        this.dashboardService = dashboardService;
    }

    @GetMapping("/api/dashboard/summary")
    public ApiResult<Map<String, Object>> summary() {
        return handle(new Callable<Map<String, Object>>() {
            @Override
            public Map<String, Object> call() {
                return dashboardService.querySummary();
            }
        });
    }

    @GetMapping("/api/dashboard/trade-trend")
    public ApiResult<List<Map<String, Object>>> tradeTrend() {
        return handle(new Callable<List<Map<String, Object>>>() {
            @Override
            public List<Map<String, Object>> call() {
                return dashboardService.queryTradeTrend();
            }
        });
    }

    @GetMapping("/api/dashboard/traffic-trend")
    public ApiResult<List<Map<String, Object>>> trafficTrend() {
        return handle(new Callable<List<Map<String, Object>>>() {
            @Override
            public List<Map<String, Object>> call() {
                return dashboardService.queryTrafficTrend();
            }
        });
    }

    @GetMapping("/api/dashboard/sku-topn")
    public ApiResult<List<Map<String, Object>>> skuTopN(@RequestParam(value = "limit", required = false) final Integer limit) {
        return handle(new Callable<List<Map<String, Object>>>() {
            @Override
            public List<Map<String, Object>> call() {
                return dashboardService.querySkuTopN(limit);
            }
        });
    }

    @GetMapping("/api/dashboard/province-topn")
    public ApiResult<List<Map<String, Object>>> provinceTopN(@RequestParam(value = "limit", required = false) final Integer limit) {
        return handle(new Callable<List<Map<String, Object>>>() {
            @Override
            public List<Map<String, Object>> call() {
                return dashboardService.queryProvinceTopN(limit);
            }
        });
    }

    @GetMapping("/api/dashboard/hour-trend")
    public ApiResult<Map<String, Object>> hourTrend() {
        return handle(new Callable<Map<String, Object>>() {
            @Override
            public Map<String, Object> call() {
                return dashboardService.queryHourTrend();
            }
        });
    }

    private <T> ApiResult<T> handle(Callable<T> callable) {
        try {
            return ApiResult.ok(callable.call());
        } catch (Exception e) {
            return ApiResult.fail(e.getMessage());
        }
    }
}
