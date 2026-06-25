<template>
  <main class="dashboard">
    <header class="topbar">
      <div>
        <h1>GMall 实时指标看板</h1>
        <div class="subtitle">ADS · 30秒窗口 · 5秒刷新</div>
      </div>
      <div class="status-panel">
        <span :class="['status-dot', hasError ? 'error' : 'active']"></span>
        <span>{{ hasError ? '连接异常' : '自动刷新中' }}</span>
        <span>最近请求 {{ lastRequestTime || '--' }}</span>
        <span>数据日期 {{ summary.curDate || '--' }}</span>
        <span>最新窗口 {{ summary.latestWindowTime || '--' }}</span>
      </div>
    </header>

    <section class="kpi-grid">
      <div v-for="item in kpis" :key="item.key" class="kpi-card">
        <div class="kpi-label">{{ item.label }}</div>
        <div class="kpi-value">{{ item.value }}</div>
      </div>
    </section>

    <section class="main-grid">
      <div class="panel span-2">
        <div class="panel-title">交易 30 秒趋势</div>
        <div ref="tradeTrendChartRef" class="chart"></div>
      </div>

      <div class="panel">
        <div class="panel-title">SKU GMV Top10</div>
        <div ref="skuTopChartRef" class="chart"></div>
      </div>

      <div class="panel">
        <div class="panel-title">省份 GMV Top10</div>
        <div ref="provinceTopChartRef" class="chart"></div>
      </div>

      <div class="panel span-2">
        <div class="panel-title">流量 30 秒趋势</div>
        <div ref="trafficTrendChartRef" class="chart"></div>
      </div>

      <div class="panel">
        <div class="panel-title">小时交易趋势</div>
        <div ref="tradeHourChartRef" class="chart"></div>
      </div>

      <div class="panel">
        <div class="panel-title">小时流量趋势</div>
        <div ref="trafficHourChartRef" class="chart"></div>
      </div>
    </section>

    <div v-if="hasError" class="error-tip">{{ errorMessage }}</div>
  </main>
</template>

<script setup>
import { computed, nextTick, onBeforeUnmount, onMounted, reactive, ref } from 'vue';
import * as echarts from 'echarts';

const refreshIntervalMs = 5000;
const lastRequestTime = ref('');
const errorMessage = ref('');
const hasError = computed(() => errorMessage.value.length > 0);

const summary = reactive({
  curDate: '',
  latestWindowTime: '',
  trade: {},
  traffic: {}
});

const state = reactive({
  tradeTrend: [],
  trafficTrend: [],
  skuTopN: [],
  provinceTopN: [],
  hourTrend: {
    trade: [],
    traffic: []
  }
});

const tradeTrendChartRef = ref(null);
const trafficTrendChartRef = ref(null);
const skuTopChartRef = ref(null);
const provinceTopChartRef = ref(null);
const tradeHourChartRef = ref(null);
const trafficHourChartRef = ref(null);

const charts = {};
let timer = null;

const kpis = computed(() => [
  { key: 'gmv', label: 'GMV', value: formatMoney(summary.trade.gmv) },
  { key: 'order', label: '下单次数', value: formatNumber(summary.trade.sku_order_count) },
  { key: 'skuNum', label: '下单件数', value: formatNumber(summary.trade.sku_num) },
  { key: 'skuCount', label: '下单SKU数', value: formatNumber(summary.trade.ordered_sku_count) },
  { key: 'pv', label: 'PV', value: formatNumber(summary.traffic.pv_ct) },
  { key: 'uv', label: 'UV', value: formatNumber(summary.traffic.uv_ct) },
  { key: 'sv', label: 'SV', value: formatNumber(summary.traffic.sv_ct) }
]);

onMounted(async () => {
  initCharts();
  await refreshAll();
  timer = window.setInterval(refreshAll, refreshIntervalMs);
  window.addEventListener('resize', resizeCharts);
});

onBeforeUnmount(() => {
  if (timer) {
    window.clearInterval(timer);
  }
  window.removeEventListener('resize', resizeCharts);
  Object.values(charts).forEach((chart) => chart.dispose());
});

function initCharts() {
  charts.tradeTrend = echarts.init(tradeTrendChartRef.value);
  charts.trafficTrend = echarts.init(trafficTrendChartRef.value);
  charts.skuTop = echarts.init(skuTopChartRef.value);
  charts.provinceTop = echarts.init(provinceTopChartRef.value);
  charts.tradeHour = echarts.init(tradeHourChartRef.value);
  charts.trafficHour = echarts.init(trafficHourChartRef.value);
}

async function refreshAll() {
  lastRequestTime.value = formatClock(new Date());
  try {
    const [summaryRes, tradeTrendRes, trafficTrendRes, skuTopRes, provinceTopRes, hourTrendRes] = await Promise.all([
      requestJson('/api/dashboard/summary'),
      requestJson('/api/dashboard/trade-trend'),
      requestJson('/api/dashboard/traffic-trend'),
      requestJson('/api/dashboard/sku-topn?limit=10'),
      requestJson('/api/dashboard/province-topn?limit=10'),
      requestJson('/api/dashboard/hour-trend')
    ]);

    Object.assign(summary, summaryRes || {});
    state.tradeTrend = tradeTrendRes || [];
    state.trafficTrend = trafficTrendRes || [];
    state.skuTopN = skuTopRes || [];
    state.provinceTopN = provinceTopRes || [];
    state.hourTrend = hourTrendRes || { trade: [], traffic: [] };
    errorMessage.value = '';

    await nextTick();
    renderCharts();
  } catch (error) {
    errorMessage.value = error.message || '看板数据查询失败';
  }
}

async function requestJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  const result = await response.json();
  if (!result.success) {
    throw new Error(result.error || '接口返回失败');
  }
  return result.data;
}

function renderCharts() {
  renderTradeTrend();
  renderTrafficTrend();
  renderSkuTop();
  renderProvinceTop();
  renderTradeHour();
  renderTrafficHour();
}

function renderTradeTrend() {
  const rows = state.tradeTrend;
  charts.tradeTrend.setOption(baseLineOption({
    x: rows.map((row) => formatTimeLabel(row.stt)),
    series: [
      { name: 'GMV', data: rows.map((row) => Number(row.gmv || 0)), smooth: true },
      { name: '下单次数', data: rows.map((row) => Number(row.sku_order_count || 0)), smooth: true }
    ],
    yName: '交易'
  }));
}

function renderTrafficTrend() {
  const rows = state.trafficTrend;
  charts.trafficTrend.setOption(baseLineOption({
    x: rows.map((row) => formatTimeLabel(row.stt)),
    series: [
      { name: 'PV', data: rows.map((row) => Number(row.pv_ct || 0)), smooth: true },
      { name: 'UV', data: rows.map((row) => Number(row.uv_ct || 0)), smooth: true },
      { name: 'SV', data: rows.map((row) => Number(row.sv_ct || 0)), smooth: true }
    ],
    yName: '流量'
  }));
}

function renderSkuTop() {
  const rows = [...state.skuTopN].reverse();
  charts.skuTop.setOption(baseBarOption({
    y: rows.map((row) => shortText(row.sku_name || row.sku_id, 14)),
    data: rows.map((row) => Number(row.order_amount || 0)),
    color: '#21c7a8'
  }));
}

function renderProvinceTop() {
  const rows = [...state.provinceTopN].reverse();
  charts.provinceTop.setOption(baseBarOption({
    y: rows.map((row) => `省份${row.province_id}`),
    data: rows.map((row) => Number(row.gmv || 0)),
    color: '#f2b84b'
  }));
}

function renderTradeHour() {
  const rows = state.hourTrend.trade || [];
  charts.tradeHour.setOption(baseLineOption({
    x: rows.map((row) => row.hour_time),
    series: [
      { name: 'GMV', data: rows.map((row) => Number(row.gmv || 0)), smooth: true },
      { name: '下单次数', data: rows.map((row) => Number(row.sku_order_count || 0)), smooth: true }
    ],
    yName: '小时交易'
  }));
}

function renderTrafficHour() {
  const rows = state.hourTrend.traffic || [];
  charts.trafficHour.setOption(baseLineOption({
    x: rows.map((row) => row.hour_time),
    series: [
      { name: 'PV', data: rows.map((row) => Number(row.pv_ct || 0)), smooth: true },
      { name: 'UV', data: rows.map((row) => Number(row.uv_ct || 0)), smooth: true }
    ],
    yName: '小时流量'
  }));
}

function baseLineOption({ x, series, yName }) {
  return {
    color: ['#41a7ff', '#21c7a8', '#f2b84b'],
    tooltip: { trigger: 'axis', backgroundColor: '#102233', borderColor: '#28465e', textStyle: { color: '#e8f1f7' } },
    legend: { top: 4, textStyle: { color: '#a7b7c7' } },
    grid: { left: 46, right: 24, top: 44, bottom: 32 },
    xAxis: {
      type: 'category',
      data: x,
      axisLabel: { color: '#8ea3b7' },
      axisLine: { lineStyle: { color: '#28465e' } }
    },
    yAxis: {
      type: 'value',
      name: yName,
      nameTextStyle: { color: '#8ea3b7' },
      axisLabel: { color: '#8ea3b7' },
      splitLine: { lineStyle: { color: '#20394d' } }
    },
    series: series.map((item) => ({ ...item, type: 'line', symbolSize: 6 }))
  };
}

function baseBarOption({ y, data, color }) {
  return {
    color: [color],
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' }, backgroundColor: '#102233', borderColor: '#28465e', textStyle: { color: '#e8f1f7' } },
    grid: { left: 88, right: 18, top: 20, bottom: 24 },
    xAxis: {
      type: 'value',
      axisLabel: { color: '#8ea3b7' },
      splitLine: { lineStyle: { color: '#20394d' } }
    },
    yAxis: {
      type: 'category',
      data: y,
      axisLabel: { color: '#c8d5e1' },
      axisLine: { lineStyle: { color: '#28465e' } }
    },
    series: [{ type: 'bar', data, barMaxWidth: 18 }]
  };
}

function resizeCharts() {
  Object.values(charts).forEach((chart) => chart.resize());
}

function formatMoney(value) {
  const number = Number(value || 0);
  return number.toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function formatNumber(value) {
  return Number(value || 0).toLocaleString('zh-CN');
}

function formatClock(date) {
  return date.toLocaleTimeString('zh-CN', { hour12: false });
}

function formatTimeLabel(value) {
  if (!value) {
    return '';
  }
  return String(value).slice(11, 19);
}

function shortText(value, maxLength) {
  const text = String(value || '');
  return text.length > maxLength ? `${text.slice(0, maxLength)}...` : text;
}
</script>
