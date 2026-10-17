function updateSetupControls() {
  const disabled = !!lastSnapshot.read_only || !!lastController.running || setupBusy;
  $("#saveSetupBtn").disabled = disabled || !setupLoaded;
  document.querySelectorAll("#setupForm input").forEach((x) => (x.disabled = disabled));
  if (lastSnapshot.read_only) $("#setupStatus").textContent = "只读预览：配置保存已禁用。";
  else if (lastController.running)
    $("#setupStatus").textContent = "机器人运行中，停止后可修改钱包配置。";
}
function renderSetup(data) {
  const wallets = data.wallets || [];
  $("#setupCredentialNote").textContent = data.credential_note || "";
  $("#setupCredentialNote").hidden = !data.credential_note;
  $("#setupWallets").innerHTML = wallets.length
    ? wallets
        .map(
          (w) =>
            `<div class="wallet-item"><strong>${esc(w.id)}</strong> <span>· ${w.enabled ? "已启用" : "未启用"} · ${w.has_private_key ? "已保存私钥" : "未填写私钥"}</span><br><span>${esc(w.account_address_masked || "地址由私钥推导")}</span>${w.storage_message ? `<br><span class="${w.storage_encrypted ? "green" : "amber"}">${esc(w.storage_message)}</span>` : ""}</div>`,
        )
        .join("")
    : '<span class="muted">尚未配置钱包</span>';
  $("#setupStatus").textContent = wallets.length
    ? "已读取本机配置；可新增或更新同名钱包。"
    : "填写钱包信息后保存；保存不会启动交易。";
  updateSetupControls();
}
async function loadSetup() {
  if (setupLoading) return;
  setupLoading = true;
  try {
    const response = await fetch("/api/setup", {
      headers: { "X-Entropy-Control": CONTROL_TOKEN },
      cache: "no-store",
    });
    if (!response.ok) {
      const failure = await response.json().catch(() => null);
      throw new Error(failure?.error || "配置读取失败（" + response.status + "）");
    }
    const data = await response.json();
    setupLoaded = true;
    renderSetup(data);
  } catch (e) {
    $("#setupStatus").textContent = e.message;
    $("#setupWallets").textContent = "本机配置暂不可用";
    updateSetupControls();
  } finally {
    setupLoading = false;
  }
}
async function saveSetup(event) {
  event.preventDefault();
  if (lastSnapshot.read_only || lastController.running || setupBusy || !setupLoaded) return;
  for (const id of ["#setupWalletId", "#setupAccountAddress", "#setupPrivateKey"])
    if (!$(id).reportValidity()) return;
  setupBusy = true;
  updateSetupControls();
  $("#setupStatus").textContent = "正在保存本机钱包配置……";
  try {
    await postControl("/api/setup", {
      wallet_id: $("#setupWalletId").value.trim(),
      account_address: $("#setupAccountAddress").value.trim(),
      private_key: $("#setupPrivateKey").value.trim(),
    });
    $("#setupPrivateKey").value = "";
    $("#setupAccountAddress").value = "";
    await loadSetup();
    $("#setupStatus").textContent = "已保存本机配置，未启动交易。";
    showToast("钱包配置已保存；未启动交易");
    await refresh();
  } catch (e) {
    const message = "配置保存失败：" + (e.message || "请检查输入与本机配置");
    $("#setupStatus").textContent = message;
    showToast(message, true);
  } finally {
    setupBusy = false;
    updateSetupControls();
  }
}
$("#setupForm").addEventListener("submit", saveSetup);
