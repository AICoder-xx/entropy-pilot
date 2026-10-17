const budgetInputs = {
  order_notional_usd: "budgetOrder",
  max_position_usd: "budgetPosition",
  max_open_order_usd: "budgetOpenOrders",
  max_drawdown_usd: "budgetDrawdown",
};
function budgetSymbol() {
  return $("#budgetMarket").value || "SNDK";
}
function currentBudget() {
  return budgetSnapshot.markets?.[budgetSymbol()] || null;
}
function budgetReason() {
  const config = currentBudget();
  return lastSnapshot.read_only || budgetSnapshot.read_only
    ? "只读模式，不能保存预算。"
    : lastController.running || budgetSnapshot.running
      ? "机器人运行中，停止后可保存固定预算。"
      : config?.editable === false
        ? config.reason || "所选市场当前不可修改。"
        : "";
}
function updateBudgetControls() {
  const config = currentBudget(),
    blocked = !!budgetReason(),
    disabled = busy || budgetBusy || !budgetLoaded || !config || blocked;
  $("#saveBudgetBtn").disabled = disabled;
  $("#budgetMarket").disabled = budgetBusy;
  for (const id of Object.values(budgetInputs)) $("#" + id).disabled = disabled;
  const reason = budgetReason();
  if (reason) $("#budgetStatus").textContent = reason;
}
function captureBudgetDraft() {
  const symbol = budgetSymbol();
  budgetDrafts[symbol] = Object.fromEntries(
    Object.entries(budgetInputs).map(([key, id]) => [key, $("#" + id).value]),
  );
  budgetDirtyMarkets.add(symbol);
  $("#budgetStatus").textContent = "已修改，尚未保存；刷新不会覆盖输入。";
}
function renderBudget(data) {
  budgetSnapshot = data || {};
  const symbol = budgetSymbol(),
    config = currentBudget();
  $("#budgetScope").textContent = `${symbol} · 全部启用钱包分别使用该预算 · 下次启动生效`;
  if (!config) {
    $("#budgetConfigNote").textContent = "所选市场尚无可读取的配置。";
    updateBudgetControls();
    return;
  }
  const values = budgetDirtyMarkets.has(symbol) ? budgetDrafts[symbol] : config.values || {};
  for (const [key, id] of Object.entries(budgetInputs)) $("#" + id).value = values?.[key] ?? "";
  const automatic = !!config.auto_scale_with_equity;
  $("#budgetConfigNote").textContent =
    (automatic
      ? "当前按权益自动缩放；输入为配置原值。保存将关闭所选市场自动缩放。"
      : "当前为固定预算；保存只修改下次启动配置，不会启动交易。") +
    " 单侧总额仅用于做市；仓位、挂单及回撤上限也约束该市场网格。";
  $("#budgetStatus").textContent = budgetDirtyMarkets.has(symbol)
    ? "已修改，尚未保存；刷新不会覆盖输入。"
    : "已读取下次启动配置。";
  updateBudgetControls();
}
async function budgetApi(method, body) {
  if (method === "POST" && lastSnapshot.read_only) throw new Error("只读模式，不能保存预算。");
  const response = await fetch("/api/budget", {
      method,
      cache: "no-store",
      headers: {
        "Content-Type": "application/json",
        "X-Entropy-Control": CONTROL_TOKEN,
      },
      ...(body ? { body: JSON.stringify(body) } : {}),
    }),
    raw = await response.text();
  let data;
  try {
    data = JSON.parse(raw);
  } catch {}
  if (!response.ok) throw new Error(data?.error || raw || `预算请求失败（${response.status}）`);
  if (!data || data.ok === false) throw new Error(data?.error || "预算响应不完整");
  return data;
}
async function loadBudget() {
  if (budgetLoading) return;
  budgetLoading = true;
  try {
    const data = await budgetApi("GET");
    budgetLoaded = true;
    budgetLastFetched = Date.now();
    renderBudget(data);
  } catch (e) {
    budgetLastFetched = Date.now();
    $("#budgetStatus").textContent = "预算读取失败：" + e.message;
    updateBudgetControls();
  } finally {
    budgetLoading = false;
  }
}
async function saveBudget(event) {
  event.preventDefault();
  if (busy || budgetBusy || !budgetLoaded || !currentBudget() || budgetReason()) return;
  for (const id of Object.values(budgetInputs)) if (!$("#" + id).reportValidity()) return;
  const symbol = budgetSymbol(),
    values = Object.fromEntries(
      Object.entries(budgetInputs).map(([key, id]) => [key, Number($("#" + id).value)]),
    );
  budgetBusy = true;
  updateControlButtons();
  $("#budgetStatus").textContent = "正在保存固定预算……";
  try {
    const data = await budgetApi("POST", {
      symbol,
      values,
      fixed_budget: true,
    });
    budgetDirtyMarkets.delete(symbol);
    delete budgetDrafts[symbol];
    if (data.markets) {
      budgetLoaded = true;
      budgetLastFetched = Date.now();
      renderBudget(data);
    } else await loadBudget();
    $("#budgetStatus").textContent = `${symbol} 固定预算已保存，下次启动生效；未启动交易。`;
    showToast(`${symbol} 固定预算已保存；未启动交易`);
    await refresh();
  } catch (e) {
    $("#budgetStatus").textContent = "预算保存失败：" + e.message;
    showToast("预算保存失败：" + e.message, true);
  } finally {
    budgetBusy = false;
    updateControlButtons();
  }
}
$("#budgetForm").addEventListener("submit", saveBudget);
$("#budgetMarket").addEventListener("change", () => renderBudget(budgetSnapshot));
for (const id of Object.values(budgetInputs))
  $("#" + id).addEventListener("input", captureBudgetDraft);
