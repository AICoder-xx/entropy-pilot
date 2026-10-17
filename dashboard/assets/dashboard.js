function syncProfiles(d) {
  const sel = $("#profile"),
    ids = (d.profiles || []).map((x) => x.id);
  if (sel.options.length !== ids.length || [...sel.options].some((o, i) => o.value !== ids[i]))
    sel.innerHTML = (d.profiles || [])
      .map((x) => {
        const mode = x.mode === "live" ? "实盘" : "模拟",
          strategy = x.strategy === "grid" ? "网格" : "双边做市",
          wallet = x.wallet_id && x.wallet_id !== "primary" ? ` · ${esc(x.wallet_id)}` : "";
        return `<option value="${esc(x.id)}">${esc(x.coin || "未知标的")}${wallet} · ${mode} · ${strategy} · ${esc(x.status || "")}</option>`;
      })
      .join("");
  selectedProfile = d.selected || "";
  sel.value = selectedProfile;
}
function render(d) {
  lastSnapshot = d;
  syncProfiles(d);
  const changed = renderControl(d.controller || {}) || syncGridProfile(),
    matching = selectedStrategy !== "grid" || summaryMatchesTarget(d.summary);
  renderLive(matching ? d.live : null, matching ? d.summary : null);
  renderGridAccounting(d.summary);
  renderCharts(matching ? d.charts : null);
  if (matching && d.live?.ok) {
    renderOrders(d.live.open_orders || []);
    renderExchangeFills(d.live.recent_fills || []);
  } else {
    $("#orderCount").textContent = "—";
    $("#orders").innerHTML = '<div class="empty">账户数据不可用，暂无法确认挂单</div>';
    $("#exchangeFills").innerHTML =
      '<div class="empty">账户数据不可用，暂无法确认最近 24 小时成交</div>';
  }
  renderBotFills(matching ? d.fills || [] : []);
  const lines = matching ? (d.bot_log || []).join("\n") : "",
    note = summaryIsCurrent(d.summary)
      ? ""
      : "[所选策略当前未运行；以下为历史日志，逐行时间以原记录为准]\n";
  $("#log").textContent = lines ? note + lines : "暂无该市场日志";
  if (!matching) showGridSelectionView();
  if (changed) void refresh();
}
async function refresh() {
  const sequence = ++refreshSequence;
  try {
    const u =
      "/api/snapshot" + (selectedProfile ? "?profile=" + encodeURIComponent(selectedProfile) : "");
    const r = await fetch(u, { cache: "no-store" });
    if (!r.ok) throw new Error(await r.text());
    const data = await r.json();
    if (sequence === refreshSequence) {
      render(data);
      if (!setupLoaded && !setupLoading) void loadSetup();
      if (
        !budgetLoading &&
        !budgetBusy &&
        (!budgetLoaded || Date.now() - budgetLastFetched > 15000)
      )
        void loadBudget();
    }
  } catch (e) {
    if (sequence === refreshSequence) {
      $("#statusLine").textContent = "面板读取失败：" + e.message;
      $("#freshBadge").className = "badge bad";
    }
  }
}
async function postControl(path, body = {}) {
  if (lastSnapshot.read_only) throw new Error("只读预览不允许修改配置或控制交易");
  const r = await fetch(path, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Entropy-Control": CONTROL_TOKEN,
    },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}
async function control(path, body = {}) {
  busy = true;
  updateControlButtons();
  try {
    await postControl(path, body);
    showToast(path.endsWith("start") ? "机器人启动请求已提交" : "已请求安全撤单停止");
  } catch (e) {
    showToast(e.message, true);
  } finally {
    busy = false;
    await refresh();
  }
}
function gridPayload() {
  const target = gridTarget();
  if (!target) throw new Error("请先选择钱包和市场");
  const inputs = ["#gridLevels", "#gridSpacing", "#gridNotional", "#gridAnchor"];
  for (const id of inputs) if (!$(id).reportValidity()) throw new Error("请检查网格参数");
  return {
    wallet_id: target.wallet_id,
    symbol: target.symbol,
    grid: {
      inventory_direction: $("#gridDirection").value,
      levels_each_side: Number($("#gridLevels").value),
      spacing_bps: Number($("#gridSpacing").value),
      order_notional_usd: Number($("#gridNotional").value),
      anchor_price: $("#gridAnchor").value === "" ? null : Number($("#gridAnchor").value),
    },
  };
}
async function saveGrid(start = false) {
  busy = true;
  updateControlButtons();
  try {
    const body = gridPayload();
    await postControl("/api/control/grid/settings", body);
    gridDirty = false;
    gridDirtyFields.clear();
    if (start)
      await postControl("/api/control/market/start", {
        wallet_id: body.wallet_id,
        symbol: body.symbol,
        strategy: "grid",
      });
    showToast(start ? "已保存参数并提交该市场网格启动请求" : "该市场网格参数已保存；未启动交易");
  } catch (e) {
    showToast(e.message, true);
  } finally {
    busy = false;
    await refresh();
  }
}
