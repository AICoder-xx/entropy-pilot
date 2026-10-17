function gridTarget() {
  return (lastController.markets || {})[$("#gridTarget").value] || null;
}
function updateControlButtons() {
  const c = lastController,
    target = gridTarget(),
    grid = selectedStrategy === "grid",
    readOnly = !!lastSnapshot.read_only;
  $("#readOnlyNotice").hidden = !readOnly;
  $("#gridForm").hidden = !grid;
  $("#startBtn").disabled =
    readOnly || busy || budgetBusy || (grid ? !target || target.running : !!c.running);
  $("#startBtn").textContent = grid ? "保存并启动此市场网格实盘" : "启动全部市场做市实盘";
  $("#stopBtn").disabled = readOnly || busy || !c.running;
  $("#saveGridBtn").disabled = readOnly || busy || !target || target.running;
  document
    .querySelectorAll("#gridForm input,#gridDirection")
    .forEach((x) => (x.disabled = readOnly || busy || !!target?.running));
  document.querySelectorAll(".strategy").forEach((x) => {
    x.disabled = busy;
    x.classList.toggle("selected", x.dataset.strategy === selectedStrategy);
    x.setAttribute?.("aria-pressed", String(x.dataset.strategy === selectedStrategy));
  });
  updateSetupControls();
  updateBudgetControls();
}
function renderControl(c) {
  lastController = c;
  const running = !!c.running,
    all = c.all_running !== false,
    wallets = Number(c.wallet_count || 1),
    scope = wallets > 1 ? `${wallets}钱包并行` : "双市场";
  $("#botBadge").textContent = running
    ? `${all ? scope + "运行中" : "部分进程运行"} · ${c.strategy === "grid" ? "网格" : c.strategy === "market_maker" ? "做市" : "多策略"}`
    : "机器人已停止";
  $("#botBadge").className = "badge " + (running && all ? "ok" : running ? "warn" : "");
  $("#controlMessage").textContent = c.message || "等待启动";
  const selector = $("#gridTarget"),
    keys = Object.keys(c.markets || {}),
    old = selector.value;
  if ([...selector.options].map((x) => x.value).join("|") !== keys.join("|")) {
    selector.innerHTML = keys
      .map((k) => {
        const m = c.markets[k];
        return `<option value="${esc(k)}">${esc(m.wallet_id || "primary")} / ${esc(m.symbol || k)}</option>`;
      })
      .join("");
    if (keys.includes(old)) selector.value = old;
  }
  const target = gridTarget();
  if (target && (!gridDirty || gridLoadedTarget !== selector.value)) {
    const g = target.grid_settings || {},
      values = {
        gridDirection: g.inventory_direction || "long",
        gridLevels: g.levels_each_side ?? "",
        gridSpacing: g.spacing_bps ?? "",
        gridNotional: g.order_notional_usd ?? "",
        gridAnchor: g.anchor_price ?? "",
      };
    for (const [id, value] of Object.entries(values))
      if (!gridDirty || !gridDirtyFields.has(id)) $("#" + id).value = value;
    gridLoadedTarget = selector.value;
    $("#gridSettingsStatus").textContent = gridDirty
      ? "参数已修改，尚未保存。"
      : target.running
        ? "此市场正在运行，停止后可修改参数。"
        : "已显示保存参数。首次接管可匹配现有仓位方向；已有网格变向须先空仓撤单。每单金额仍受风险限制。";
  }
  updateControlButtons();
  const r = c.risk_profile || {};
  $("#riskBudgetScope").textContent =
    c.risk_profile_source === "planned"
      ? "计划合计（只读）"
      : running
        ? "当前运行合计（只读）"
        : "上次运行合计（只读）";
  if (Object.keys(r).length)
    $("#riskProfile").innerHTML =
      `<span>订单 ${money(r.order_notional_usd, 2)}</span><span>仓位 ${money(r.max_position_usd, 2)}</span><span>挂单 ${money(r.max_open_order_usd, 2)}</span><span>回撤 ${money(r.max_drawdown_usd, 2)}</span>`;
  const profileChanged = syncGridProfile();
  updateGridLedgerStatus();
  return profileChanged;
}
function renderGridAccounting(s) {
  const target = gridTarget(),
    gridMode = selectedStrategy === "grid",
    matching =
      !gridMode ||
      (summaryMatchesTarget(s) && (s?.dashboard_profile?.strategy || s?.strategy) === "grid"),
    g = matching && s?.grid_accounting;
  $("#gridAccounting").hidden = !gridMode && !g;
  if (!g) {
    $("#gridAccountingScope").textContent = target
      ? `${target.wallet_id || "primary"} / ${target.symbol}`
      : "";
    $("#gridCards").innerHTML = "";
    $("#gridAccountingNote").textContent = gridMode
      ? `暂无 ${target?.wallet_id || "所选钱包"} / ${target?.symbol || "所选市场"} 的网格账本。完成该市场的真实库存初始化后显示；不会用其他市场或做市数据替代。`
      : "";
    return;
  }
  const state = {
    ready: "已对账",
    unknown: "需要对账",
    reconciliation_required: "需要对账",
    error: "账本异常",
    uninitialized: "等待初始化",
  };
  $("#gridAccountingScope").textContent =
    `${s.coin || ""} · ${g.direction === "short" ? "空头库存" : "多头库存"} · ${state[g.status] || g.status || "未知"} · ${time(g.inception_time_ms)} 起 · ${summaryIsCurrent(s) ? "当前账本" : "历史账本"}截止 ${time(s.updated_at_ms)}`;
  $("#gridCards").innerHTML = [
    card("真实库存数量", num(g.inventory_qty, 8), "有符号数量：多仓为正、空仓为负"),
    card(
      "真实持仓均价",
      num(g.average_entry_price, 6),
      `持仓成本名义额 ${money(g.inventory_cost_usd)}`,
    ),
    card(
      "网格配对净收益（账面）",
      money(g.grid_pair_net_usd),
      "配对价差 − 两腿已知手续费；不代表整体盈利",
      cls(g.grid_pair_net_usd),
    ),
    card(
      "真实已实现净盈亏",
      money(g.realized_net_usd),
      "按全库存均价结算 − 已知手续费 + 已入账资金费",
      cls(g.realized_net_usd),
    ),
    card(
      "已实现盈亏（未含资金费）",
      money(g.realized_after_trade_fees_usd),
      "仅交易已实现盈亏 − 已知手续费",
      cls(g.realized_after_trade_fees_usd),
    ),
    card(
      "库存浮动盈亏",
      money(g.unrealized_usd),
      "剩余库存按最新标记价计价",
      cls(g.unrealized_usd),
    ),
    card(
      "真实总净盈亏",
      money(g.total_net_usd),
      "真实已实现净盈亏 + 库存浮动盈亏",
      cls(g.total_net_usd),
    ),
    card(
      "计入累计盈亏后的回本价",
      num(g.break_even_price, 6),
      "多头获利使回本价下降；空头获利使回本价上升",
    ),
    card("等效剩余回本成本", money(g.net_invested_usd), "含累计真实收支；不是保证金或可提现金额"),
    card(
      "实际已知费用 / 资金费",
      `${money(g.fees_paid_usd)} / ${money(g.funding_net_usd)}`,
      g.funding_complete
        ? `资金费已核对至 ${time(g.funding_coverage_until_ms)}`
        : "资金费覆盖尚未完整，完整净盈亏与回本价不显示",
    ),
    card(
      "配对数量 / 未配对库存",
      `${num(g.matched_qty, 6)} / ${num(g.unmatched_qty, 6)}`,
      `${num(g.fill_count, 0)} 笔已入账成交 · ${num(g.funding_event_count, 0)} 笔资金费`,
    ),
  ].join("");
  $("#gridAccountingNote").textContent =
    (g.reason ? g.reason + "。" : "") +
    "配对盈利可能与真实已实现亏损同时出现：低价新增库存完成一轮价差，减仓仍按整体库存成本结算。配对收益不重复加入真实总盈亏。已知实际收益可改善剩余库存回本价，但可提现金额仍由交易所保证金规则决定。统计自账本建立起，接管旧持仓前的历史费用不包含；预测返佣不计入真实收益。";
}

function summaryController(s) {
  return (
    Object.values(lastController.markets || {}).find(
      (m) =>
        (m.wallet_id || "primary") === (s?.wallet_id || "primary") &&
        m.symbol ===
          String(s?.coin || "")
            .split(":")
            .pop(),
    ) || null
  );
}
function summaryIsCurrent(s) {
  const bot = summaryController(s),
    strategy = s?.dashboard_profile?.strategy || s?.strategy;
  return !!(
    bot?.running &&
    bot.strategy === strategy &&
    bot.run_id &&
    s?.session_id === bot.run_id &&
    ["running", "paused"].includes(s?.status)
  );
}
function summaryMatchesTarget(s) {
  const target = gridTarget();
  return !!(
    s &&
    target &&
    (s.wallet_id || "primary") === (target.wallet_id || "primary") &&
    String(s.coin || "")
      .split(":")
      .pop() === target.symbol
  );
}
function syncGridProfile() {
  if (selectedStrategy !== "grid") return false;
  const target = gridTarget();
  if (!target) return false;
  const matches = (lastSnapshot.profiles || []).filter(
      (p) =>
        p.mode === "live" &&
        (p.wallet_id || "primary") === (target.wallet_id || "primary") &&
        String(p.coin || "")
          .split(":")
          .pop() === target.symbol,
    ),
    profile = matches.find((p) => p.strategy === "grid") || matches[0];
  if (!profile || profile.id === selectedProfile) return false;
  selectedProfile = profile.id;
  $("#profile").value = profile.id;
  return true;
}
function gridProfilesForTarget() {
  const target = gridTarget();
  return target
    ? (lastSnapshot.profiles || []).filter(
        (p) =>
          p.mode === "live" &&
          p.strategy === "grid" &&
          (p.wallet_id || "primary") === (target.wallet_id || "primary") &&
          String(p.coin || "")
            .split(":")
            .pop() === target.symbol,
      )
    : [];
}
function updateGridLedgerStatus() {
  const target = gridTarget();
  if (!target) {
    $("#gridLedgerStatus").textContent = "正在读取钱包和市场；尚未取得对应网格账本。";
    return;
  }
  const label = `${target.wallet_id || "primary"} / ${target.symbol}`,
    s = lastSnapshot.summary,
    matching =
      summaryMatchesTarget(s) &&
      (s?.dashboard_profile?.strategy || s?.strategy) === "grid" &&
      s?.grid_accounting;
  $("#gridLedgerStatus").textContent = matching
    ? `${label} 网格账本已读取，统计截止 ${time(s.updated_at_ms)}。`
    : gridProfilesForTarget().length
      ? `正在读取 ${label} 的网格账本；不会显示其他市场数据。`
      : `暂无 ${label} 的网格账本。完成该市场真实库存初始化后显示。`;
}
function showGridSelectionView() {
  updateGridLedgerStatus();
  renderGridAccounting(lastSnapshot.summary);
  if (selectedStrategy !== "grid" || summaryMatchesTarget(lastSnapshot.summary)) return;
  const target = gridTarget(),
    label = target ? `${target.wallet_id || "primary"} / ${target.symbol}` : "所选钱包 / 市场";
  const hasProfile =
    !!target &&
    (lastSnapshot.profiles || []).some(
      (p) =>
        p.mode === "live" &&
        (p.wallet_id || "primary") === (target.wallet_id || "primary") &&
        String(p.coin || "")
          .split(":")
          .pop() === target.symbol,
    );
  renderLive(null, null);
  renderCharts(null);
  $("#statusLine").textContent = hasProfile
    ? `正在读取 ${label} 的账户与统计……`
    : `${label} 暂无运行记录；账户、挂单和成交状态尚未确认。`;
  $("#orderCount").textContent = "—";
  for (const id of ["orders", "exchangeFills", "botFills"])
    $("#" + id).innerHTML = hasProfile
      ? '<div class="empty">正在读取所选市场数据……</div>'
      : '<div class="empty">暂无匹配的运行记录，所选市场状态未知</div>';
  $("#log").textContent = hasProfile ? "正在读取所选市场日志……" : "暂无所选市场日志";
}
function knownSessionNetFees(sum) {
  if (sum.fee_accounting_complete === false || sum.fees_paid == null) return null;
  const paid = Number(sum.fees_paid),
    rebate = Number(sum.projected_self_rebate || 0) + Number(sum.projected_referred_benefit || 0);
  return Number.isFinite(paid) && Number.isFinite(rebate) ? paid - rebate : null;
}
