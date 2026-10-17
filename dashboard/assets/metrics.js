function renderLive(l, s) {
  const sum = s || {},
    current = summaryIsCurrent(s),
    paused = current && (sum.status === "paused" || l?.bot_status === "paused"),
    history = !!s && !current,
    prof = sum.dashboard_profile || {},
    asOf = time(sum.updated_at_ms),
    sessionScope = s ? `${time(sum.session_started_at_ms)} — ${asOf}` : "暂无策略统计",
    runLabel = current ? (paused ? "当前会话暂停挂单" : "当前会话运行中") : "上次运行（历史摘要）",
    coinLabel = `${sum.coin || ""} · ${prof.strategy === "grid" ? "网格" : "做市"}`,
    agg = sum.dashboard_aggregates || {},
    reb = (sum.projected_self_rebate || 0) + (sum.projected_referred_benefit || 0),
    elapsed = Math.max(
      0,
      (Number(sum.updated_at_ms || 0) - Number(sum.session_started_at_ms || 0)) / 1000,
    ),
    daily = elapsed > 0 ? (Number(sum.volume_usd || 0) / elapsed) * 86400 : null;
  let liveCards = metricSection(
    "账户与市场快照",
    "尚未读取",
    '<div class="empty" style="grid-column:1/-1">账户数据暂不可用，请查看顶部状态。</div>',
  );
  if (!l || !l.ok) {
    $("#walletBadge").textContent = "账户数据不可用";
    $("#walletBadge").className = "badge warn";
    $("#statusLine").textContent = l?.error || "所选市场账户数据尚未读取";
    $("#freshBadge").textContent = "等待账户快照";
    $("#freshBadge").className = "badge warn";
    $("#exposure").innerHTML = '<div class="empty">账户数据不可用</div>';
  } else {
    const m = l.market || {},
      p = l.position || {},
      localTs = Number(l.local_telemetry_updated_at_ms || 0),
      officialTs = Number(l.updated_at_ms || 0),
      accountTime = localTs || officialTs,
      source = localTs ? "脚本账户快照" : "官方账户快照",
      netPos = Number((l.exposure || {}).net_position_usd || 0),
      liqDist =
        p.liquidation_px != null && m.mark_px
          ? (Math.abs(Number(m.mark_px) - Number(p.liquidation_px)) / Number(m.mark_px)) * 100
          : null;
    $("#walletBadge").textContent = paused
      ? "策略暂停挂单"
      : `账户已读取 · ${l.account_abstraction || "—"}`;
    $("#walletBadge").className = "badge " + (paused ? "warn" : "ok");
    $("#freshBadge").textContent = `${source} ${time(accountTime)}`;
    $("#freshBadge").className = "badge " + (l.telemetry_stale ? "warn" : "ok");
    $("#statusLine").textContent =
      `${l.coin || ""} · bid/ask ${num(m.best_bid, 4)} / ${num(m.best_ask, 4)} · ${source} ${time(accountTime)}${history ? " · 下方策略统计为历史摘要" : ""}`;
    liveCards = metricSection(
      "账户与市场快照",
      `${l.coin || ""} · ${source} · ${time(accountTime)}`,
      [
        card(
          "账户权益",
          money(l.monitored_equity_usd),
          `${esc(l.collateral_source || source)}`,
          "blue",
        ),
        card("真实净仓位", money(netPos), `${num(p.qty, 4)} @ ${num(p.entry_px, 2)}`, cls(netPos)),
        card(
          "可用保证金",
          money(l.free_collateral_usd),
          `可买 ${money(l.available_to_buy_usd, 0)} · 可卖 ${money(l.available_to_sell_usd, 0)}`,
        ),
        card(
          "当前持仓浮盈亏",
          money(p.unrealized_pnl_usd),
          "当前仓位按标记价计算",
          cls(p.unrealized_pnl_usd),
        ),
        card(
          "强平距离",
          liqDist == null ? "无可用强平价" : `${num(liqDist, 1)}%`,
          p.liquidation_px == null ? "以交易所实际仓位为准" : `强平价 ${num(p.liquidation_px, 2)}`,
          "amber",
        ),
        card(
          "杠杆",
          p.leverage?.value ? `${num(p.leverage.value, 0)}×` : "—",
          p.leverage
            ? `${esc(p.leverage.type || "")} · 保证金 ${money(p.margin_used_usd)}`
            : "当前无仓位",
        ),
        card(
          "标记 / Oracle",
          num(m.mark_px, 2),
          `Oracle ${num(m.oracle_px, 2)} · 资金费率 ${m.funding == null ? "—" : num(Number(m.funding) * 100, 4) + "%"}`,
        ),
        card(
          "盘口价差",
          m.spread_bps == null ? "—" : `${num(m.spread_bps, 2)} bps`,
          `bid ${num(m.best_bid, 2)} / ask ${num(m.best_ask, 2)}`,
        ),
        card("市场 24h 总成交", money(m.day_notional_volume, 0), "全市场口径，非本账户", "purple"),
      ].join(""),
    );
    renderExposure(l.exposure || {});
  }
  if (!s) {
    $("#cards").innerHTML =
      liveCards +
      metricSection(
        "策略统计",
        "暂无所选记录",
        '<div class="empty" style="grid-column:1/-1">暂无该钱包与市场的策略摘要。</div>',
        true,
      );
    return;
  }
  const rp = sum.risk_profile || {},
    am = sum.adaptive_spread_metrics || {},
    ab = sum.action_budget || {},
    sessionPnl = sum.account_session_net_pnl_usd,
    netFee = knownSessionNetFees(sum),
    quoteState = !current
      ? summaryController(s)?.running
        ? "当前进程与此历史摘要不同"
        : "已停止（下方历史统计）"
      : paused
        ? "暂停挂单"
        : am.crisis_mode_active
          ? "危机报价中"
          : am.volatility_guard_active
            ? "波动守卫中"
            : "正常报价",
    wearBps = agg.markout_30_projected_execution_wear_bps;
  $("#cards").innerHTML =
    liveCards +
    metricSection(
      current ? "当前策略统计" : "历史策略统计",
      `${coinLabel} · ${runLabel} · 截止 ${asOf}`,
      [
        card(
          "报价状态",
          quoteState,
          current ? `当前会话统计截止 ${asOf}` : `以下数据仅属于 ${sessionScope}，不代表当前交易`,
          current && !paused ? "green" : "amber",
        ),
        card(
          current ? "本次运行盈亏" : "上次运行盈亏",
          money(sessionPnl),
          `统计期间 ${sessionScope} · 会话停机线 −${money(rp.max_drawdown_usd, 0)}`,
          cls(sessionPnl),
        ),
        card(
          current ? "本金累计盈亏" : "上次摘要本金累计盈亏",
          money(sum.principal_net_pnl_usd),
          `截止 ${asOf} · 摘要权益 ${money(sum.equity_actual)} − 本金 ${money(sum.configured_capital_usd, 0)}`,
          cls(sum.principal_net_pnl_usd),
        ),
        card(
          "会话成交额",
          money(sum.volume_usd),
          `${sum.fill_count ?? "—"} 笔 · ${sessionScope}`,
          "purple",
        ),
        card(
          current ? "24h 量投影（运行市场合计）" : "上次会话成交量日化（历史外推）",
          money(current ? agg.portfolio_projected_24h_volume_usd : daily, 0),
          current
            ? `本标的 ${money(daily, 0)} · 按已运行时长线性外推`
            : `仅用历史 ${num(elapsed / 60, 0)} 分钟外推，当前未运行`,
          "purple",
        ),
        card(
          "会话磨损率",
          wearBps == null ? "—" : `${num(wearBps, 2)} bps`,
          `30 秒口径 ${agg.markout_30_samples || 0} 样本 · 截止 ${asOf}`,
          cls(agg.markout_30_projected_execution_wear_usd, true),
        ),
        card(
          "会话磨损金额",
          money(sum.actual_wear_usd),
          `摘要已实现 ${money(sum.realized_pnl)} / 摘要未实现 ${money(sum.unrealized_pnl)} · 截止 ${asOf}`,
          cls(sum.actual_wear_usd, true),
        ),
        card(
          "会话净手续费（含投影返佣）",
          money(netFee),
          netFee == null
            ? `费用尚未完整核验，净手续费未知 · 截止 ${asOf}`
            : `手续费 ${money(sum.fees_paid)} − 投影返佣 ${money(reb)} · 截止 ${asOf}`,
          cls(-netFee),
        ),
        card(
          "会话动作效率",
          ab.volume_per_action_usd == null ? "—" : `${money(ab.volume_per_action_usd, 2)}/动作`,
          `${num(ab.session_spent, 0)} 动作 · ${sessionScope}`,
          Number(ab.self_funding_ratio || 0) >= 1 ? "green" : "amber",
        ),
      ].join(""),
      !current,
    );
}

function renderExposure(e) {
  const s = lastSnapshot.summary,
    limit = Number(s?.risk_profile?.max_position_usd),
    valid = Number.isFinite(limit) && limit > 0,
    scope = valid
      ? `${summaryIsCurrent(s) ? "当前配置" : "所选历史记录配置"}上限 ${money(limit, 0)}`
      : "暂无所选市场仓位上限";
  function side(isLong) {
    const worst = isLong ? e.worst_case_long_usd : e.worst_case_short_usd,
      current = isLong ? e.current_long_usd : e.current_short_usd,
      pending = isLong ? e.pending_buy_usd : e.pending_sell_usd,
      pct = valid && worst != null ? (Number(worst) / limit) * 100 : null;
    return `<div class="sidebox ${isLong ? "long" : "short"}"><div class="sidehead"><span class="${isLong ? "green" : "red"}">${isLong ? "多头" : "空头"}最坏敞口</span><b>${money(worst, 2)}${pct == null ? "" : ` · ${num(pct, 1)}%`}</b></div><div class="meter"><span style="width:${pct == null ? 0 : Math.max(0, Math.min(100, pct))}%"></span></div><div class="sidegrid"><div><span>当前持仓</span><b>${money(current, 2)}</b></div><div><span>${isLong ? "买单" : "卖单"}待成交</span><b>${money(pending, 2)}</b></div><div><span>配置上限</span><b>${valid ? money(limit, 0) : "—"}</b></div></div></div>`;
  }
  $("#exposure").innerHTML =
    side(true) +
    side(false) +
    `<div class="risk-scope">${esc(scope)}；按同向挂单全部成交计算。${valid ? "" : "无上限时不绘制风险比例。"}</div>`;
}

function renderOrders(rows) {
  $("#orderCount").textContent = `${rows.length} 笔`;
  if (!rows.length) {
    $("#orders").innerHTML = '<div class="empty">当前没有挂单</div>';
    return;
  }
  let h =
    "<table><thead><tr><th>挂单时间</th><th>订单 ID</th><th>方向</th><th>价格</th><th>剩余数量</th><th>已成交</th><th>剩余金额</th><th>TIF</th><th>Reduce only</th></tr></thead><tbody>";
  for (const r of rows)
    h += `<tr><td>${time(r.timestamp_ms)}</td><td>${esc(r.oid)}<br><span class="muted">${esc(r.cloid || "")}</span></td><td class="${r.is_buy ? "green" : "red"}">${esc(r.side)}</td><td>${num(r.price, 6)}</td><td>${num(r.remaining_qty, 8)}</td><td>${num(r.filled_qty, 8)}</td><td>${money(r.notional_usd)}</td><td><span class="pill">${esc(r.tif || r.order_type || "")}</span></td><td>${r.reduce_only ? "是" : "否"}</td></tr>`;
  $("#orders").innerHTML = h + "</tbody></table>";
}

function renderExchangeFills(rows) {
  if (!rows.length) {
    $("#exchangeFills").innerHTML = '<div class="empty">最近 24 小时没有该标的成交</div>';
    return;
  }
  let h =
    "<table><thead><tr><th>时间</th><th>订单 ID</th><th>方向</th><th>开平</th><th>价格</th><th>数量</th><th>成交额</th><th>流动性</th><th>手续费</th><th>Closed PnL</th></tr></thead><tbody>";
  for (const r of rows)
    h += `<tr><td>${time(r.time_ms)}</td><td>${esc(r.oid)}</td><td class="${r.is_buy ? "green" : "red"}">${esc(r.side)}</td><td>${esc(r.direction || "")}</td><td>${num(r.price, 6)}</td><td>${num(r.qty, 8)}</td><td>${money(r.notional_usd)}</td><td><span class="pill">${esc(r.liquidity)}</span></td><td>${money(r.fee_usd)}</td><td class="${cls(r.closed_pnl_usd)}">${money(r.closed_pnl_usd)}</td></tr>`;
  $("#exchangeFills").innerHTML = h + "</tbody></table>";
}

function mark(row, h) {
  const x = row.markouts && row.markouts[String(h)];
  return x ? x.markout_bps : null;
}
function renderBotFills(rows) {
  if (!rows.length) {
    $("#botFills").innerHTML = '<div class="empty">当前脚本会话还没有成交</div>';
    return;
  }
  let h =
    "<table><thead><tr><th>时间</th><th>策略订单</th><th>方向</th><th>成交额</th><th>价格</th><th>数量</th><th>流动性</th><th>实际费</th><th>投影返佣</th><th>Closed PnL</th><th>1s</th><th>5s</th><th>30s</th><th>逐笔磨损</th></tr></thead><tbody>";
  for (const r of rows) {
    const mo = (x) => {
      const v = mark(r, x);
      return `<span class="${cls(v)}">${v == null ? "—" : num(v, 3) + " bps"}</span>`;
    };
    h += `<tr><td>${time(r.time_ms)}</td><td>${esc(r.order_role || "")}<br><span class="muted">${esc(r.oid || "")}</span></td><td class="${r.is_buy ? "green" : "red"}">${r.is_buy ? "BUY" : "SELL"}</td><td>${money(r.notional_usd)}</td><td>${num(r.price, 6)}</td><td>${num(r.qty, 8)}</td><td><span class="pill">${esc(r.liquidity || "")}</span></td><td>${money(r.fee_usd)}</td><td>${money(r.projected_rebate_total_usd)}</td><td class="${cls(r.realized_pnl_delta)}">${money(r.realized_pnl_delta)}</td><td>${mo(1)}</td><td>${mo(5)}</td><td>${mo(30)}</td><td class="${cls(r.latest_execution_wear_usd, true)}">${money(r.latest_execution_wear_usd)}</td></tr>`;
  }
  $("#botFills").innerHTML = h + "</tbody></table>";
}
