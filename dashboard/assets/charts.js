function chartNumber(value) {
  return value == null || value === "" || !Number.isFinite(Number(value)) ? null : Number(value);
}
function chartMoney(value) {
  if (!Number.isFinite(value)) return "—";
  const sign = value < 0 ? "−" : "";
  const n = Math.abs(value);
  return (
    sign +
    "$" +
    (n >= 1e6
      ? num(n / 1e6, 2) + "M"
      : n >= 1e3
        ? num(n / 1e3, n >= 1e4 ? 0 : 1) + "k"
        : num(n, n >= 10 ? 0 : 2))
  );
}
function drawSessionChart(id, rows, fields) {
  const el = $("#" + id),
    width = Math.max(290, (el.clientWidth || 760) - 22),
    height = 190,
    left = 58,
    right = 15,
    top = 12,
    bottom = 29,
    w = width - left - right,
    h = height - top - bottom,
    points = rows
      .map((r) => ({ ...r, time_ms: chartNumber(r.time_ms) }))
      .filter((r) => r.time_ms != null)
      .sort((a, b) => a.time_ms - b.time_ms),
    values = points
      .flatMap((r) => fields.map((f) => chartNumber(r[f.key])))
      .filter((x) => x != null);
  if (!points.length || !values.length) {
    el.innerHTML = '<div class="empty">所选会话没有可绘制的已知数据</div>';
    return;
  }
  const first = points[0].time_ms,
    last = points[points.length - 1].time_ms,
    span = Math.max(last - first, 1);
  let lo = Math.min(0, ...values),
    hi = Math.max(0, ...values);
  if (hi === lo) {
    hi = lo + 1;
    lo = Math.min(0, lo);
  }
  const pad = (hi - lo) * 0.07;
  hi += pad;
  if (lo < 0) lo -= pad;
  const x = (t) => left + ((t - first) / span) * w,
    y = (v) => top + ((hi - v) / (hi - lo)) * h;
  let svg = `<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="${id === "volumeChart" ? "所选会话累计成交额" : "所选会话已实现减手续费及累计手续费"}">`;
  for (let i = 0; i < 4; i++) {
    const v = lo + ((hi - lo) * i) / 3,
      yy = y(v);
    svg += `<line x1="${left}" y1="${yy}" x2="${width - right}" y2="${yy}" stroke="#e8edf3" stroke-width="1"/><text x="${left - 8}" y="${yy + 3}" text-anchor="end" font-size="9" fill="#92a1b4">${esc(chartMoney(v))}</text>`;
  }
  if (lo < 0 && hi > 0)
    svg += `<line x1="${left}" y1="${y(0)}" x2="${width - right}" y2="${y(0)}" stroke="#b6c6d8" stroke-dasharray="3 3"/>`;
  for (let i = 0; i < 4; i++) {
    const t = first + ((last - first) * i) / 3,
      xx = x(t);
    svg += `<line x1="${xx}" y1="${top}" x2="${xx}" y2="${height - bottom}" stroke="#f0f3f7"/><text x="${xx}" y="${height - 8}" text-anchor="${i === 0 ? "start" : i === 3 ? "end" : "middle"}" font-size="9" fill="#92a1b4">${new Date(t).toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit" })}</text>`;
  }
  for (const f of fields) {
    let segments = [],
      segment = [];
    for (const r of points) {
      const v = chartNumber(r[f.key]);
      if (v == null) {
        if (segment.length) segments.push(segment);
        segment = [];
      } else segment.push([x(r.time_ms), y(v), r.time_ms, v]);
    }
    if (segment.length) segments.push(segment);
    for (const seg of segments) {
      const d = seg
        .map((p, i) => `${i ? "L" : "M"}${p[0].toFixed(2)},${p[1].toFixed(2)}`)
        .join(" ");
      if (f.area && seg.length > 1)
        svg += `<path d="${d} L${seg[seg.length - 1][0]},${height - bottom} L${seg[0][0]},${height - bottom} Z" fill="#dceafb" opacity=".58"/>`;
      svg += `<path d="${d}" fill="none" stroke="${f.color}" stroke-width="1.8" stroke-linejoin="round" stroke-linecap="round"/>`;
      const last = seg[seg.length - 1];
      svg += `<circle cx="${last[0]}" cy="${last[1]}" r="2.6" fill="${f.color}"><title>${esc(f.label + " · " + time(last[2]) + " · " + money(last[3], 4))}</title></circle>`;
    }
  }
  el.innerHTML = svg + "</svg>";
}
function renderCharts(data) {
  lastCharts = data;
  const scope = data?.scope || {},
    rows = Array.isArray(data?.series) ? data.series : [],
    last = rows[rows.length - 1] || {},
    volume = chartNumber(last.cumulative_volume_usd),
    net = chartNumber(last.cumulative_realized_less_fees_usd),
    fee = chartNumber(last.cumulative_fee_usd);
  $("#chartScope").textContent = data
    ? `${scope.coin || ""} · ${scope.mode === "paper" ? "模拟" : "实盘"} · ${scope.fill_count ?? "—"} 笔`
    : "暂无所选会话数据";
  $("#chartSummary").innerHTML = [
    ["成交额", money(volume, 2), "blue"],
    ["已实现减手续费", money(net, 4), cls(net)],
    ["实际手续费", money(fee, 4), ""],
    ["成交笔数", num(scope.fill_count, 0), ""],
  ]
    .map(
      ([label, value, color]) =>
        `<div class="chart-stat"><span>${esc(label)}</span><b class="${color}">${value}</b></div>`,
    )
    .join("");
  drawSessionChart("volumeChart", rows, [
    {
      key: "cumulative_volume_usd",
      label: "累计成交额",
      color: "#4b88c8",
      area: true,
    },
  ]);
  drawSessionChart("pnlChart", rows, [
    {
      key: "cumulative_realized_less_fees_usd",
      label: "已实现减手续费",
      color: "#c06f77",
    },
    { key: "cumulative_fee_usd", label: "累计手续费", color: "#b89a69" },
  ]);
  const sides = data?.sides,
    buy = chartNumber(sides?.buy?.volume_usd),
    sell = chartNumber(sides?.sell?.volume_usd),
    total = buy != null && sell != null ? buy + sell : null;
  if (total != null && total > 0) {
    const buyPct = (buy / total) * 100;
    $("#tradeDistribution").innerHTML =
      `<div class="distribution-head"><span class="green">买入 <b>${money(buy, 2)}</b> <small>${num(sides.buy.count, 0)} 笔</small></span><span class="red">卖出 <b>${money(sell, 2)}</b> <small>${num(sides.sell.count, 0)} 笔</small></span></div><div class="buy-sell-track"><span class="buy" style="width:${buyPct}%"></span><span class="sell" style="width:${100 - buyPct}%"></span></div><div class="distribution-foot"><span>买入 ${num(buyPct, 1)}%</span><span>卖出 ${num(100 - buyPct, 1)}%</span></div>`;
  } else
    $("#tradeDistribution").innerHTML = '<div class="empty">所选会话暂无已知买卖成交金额</div>';
  if (Number(sides?.unknown?.count) > 0)
    $("#tradeDistribution").innerHTML +=
      `<div class="risk-scope">另有 ${num(sides.unknown.count, 0)} 笔方向未知，未归入买卖比例。</div>`;
  $("#chartNote").textContent =
    (data && scope.from_ms ? `${time(scope.from_ms)} — ${time(scope.to_ms)}。` : "") +
    (data?.note ||
      "仅统计已保存成交的价格已实现盈亏与实际手续费；不含未实现、资金费和返佣，不代表账户权益曲线。") +
    (scope.complete === false ? " 数据不完整，缺失部分保持未知。" : "");
}
if (typeof window !== "undefined")
  window.addEventListener("resize", () => {
    if (lastCharts) renderCharts(lastCharts);
  });
