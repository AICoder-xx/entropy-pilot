document.querySelectorAll(".strategy").forEach((btn) =>
  btn.addEventListener("click", () => {
    selectedStrategy = btn.dataset.strategy;
    updateControlButtons();
    syncGridProfile();
    showGridSelectionView();
    void refresh();
  }),
);
document.querySelectorAll("#gridForm input,#gridDirection").forEach((input) =>
  input.addEventListener("input", () => {
    gridDirty = true;
    gridDirtyFields.add(input.id);
    $("#gridSettingsStatus").textContent = "参数已修改，尚未保存。";
  }),
);
$("#gridTarget").addEventListener("change", () => {
  gridDirty = false;
  gridDirtyFields.clear();
  renderControl(lastController);
  syncGridProfile();
  showGridSelectionView();
  void refresh();
});
$("#saveGridBtn").addEventListener("click", () => saveGrid(false));
$("#startBtn").addEventListener("click", () =>
  selectedStrategy === "grid"
    ? saveGrid(true)
    : control("/api/control/start", { strategy: selectedStrategy }),
);
$("#stopBtn").addEventListener("click", () => control("/api/control/stop"));
$("#profile").addEventListener("change", (e) => {
  selectedProfile = e.target.value;
  refresh();
});
refresh();
setInterval(refresh, 2000);
