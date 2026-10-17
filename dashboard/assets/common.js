const CONTROL_TOKEN = document.querySelector("meta[name=entropy-control]").content;
const $ = (s) => document.querySelector(s);
let selectedProfile = "",
  selectedStrategy = "market_maker",
  busy = false,
  gridDirty = false,
  gridLoadedTarget = "",
  lastController = {},
  lastSnapshot = {},
  refreshSequence = 0,
  gridDirtyFields = new Set(),
  setupLoaded = false,
  setupLoading = false,
  setupBusy = false,
  lastCharts = null,
  budgetSnapshot = {},
  budgetLoaded = false,
  budgetLoading = false,
  budgetBusy = false,
  budgetLastFetched = 0,
  budgetDrafts = {},
  budgetDirtyMarkets = new Set();
const esc = (v) =>
  String(v ?? "").replace(
    /[&<>"']/g,
    (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c],
  );
const num = (v, d = 2) =>
  v == null || v === "" || !Number.isFinite(Number(v))
    ? "—"
    : Number(v).toLocaleString("zh-CN", {
        minimumFractionDigits: d,
        maximumFractionDigits: d,
      });
const money = (v, d = 4) =>
  v == null || v === "" ? "—" : `${Number(v) < 0 ? "−" : ""}$${num(Math.abs(Number(v)), d)}`;
const cls = (v, invert = false) => {
  if (v == null || Number(v) === 0) return "";
  return (invert ? Number(v) < 0 : Number(v) > 0) ? "green" : "red";
};
const time = (v) => (v ? new Date(Number(v)).toLocaleString() : "—");
function card(label, value, sub = "", klass = "") {
  return `<div class="card"><div class="label">${esc(label)}</div><div class="value ${klass}">${value}</div><div class="small">${sub}</div></div>`;
}
function metricSection(title, scope, content, history = false) {
  return `<section class="metric-group${history ? " history" : ""}"><div class="metric-group-head"><div class="metric-group-title"><span class="scope-dot"></span>${esc(title)}</div><span class="metric-group-sub">${esc(scope)}</span></div><div class="metric-grid">${content}</div></section>`;
}
function showToast(message, bad = false) {
  const t = $("#toast");
  t.textContent = message;
  t.className = "toast" + (bad ? " bad" : "");
  t.style.display = "block";
  setTimeout(() => (t.style.display = "none"), 5000);
}
