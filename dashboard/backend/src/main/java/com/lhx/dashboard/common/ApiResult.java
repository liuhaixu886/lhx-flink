package com.lhx.dashboard.common;

/**
 * 前后端统一响应结构。
 */
public class ApiResult<T> {

    private boolean success;
    private T data;
    private String error;
    private long queryTime;

    public static <T> ApiResult<T> ok(T data) {
        ApiResult<T> result = new ApiResult<T>();
        result.success = true;
        result.data = data;
        result.queryTime = System.currentTimeMillis();
        return result;
    }

    public static <T> ApiResult<T> fail(String error) {
        ApiResult<T> result = new ApiResult<T>();
        result.success = false;
        result.error = error;
        result.queryTime = System.currentTimeMillis();
        return result;
    }

    public boolean isSuccess() {
        return success;
    }

    public T getData() {
        return data;
    }

    public String getError() {
        return error;
    }

    public long getQueryTime() {
        return queryTime;
    }
}
