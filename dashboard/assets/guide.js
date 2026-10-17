(() => {
  "use strict";
  const get = (id) => document.getElementById(id),
    center = get("guideDialog"),
    tour = get("guideTourDialog"),
    card = get("guideTourCard"),
    spot = get("guideSpotlight"),
    welcome = get("guideWelcome"),
    entry = get("guideEntry");
  if (!center || !tour || !entry) return;
  const storageKey = "entropy.beginner-guide.v1",
    motion = window.matchMedia("(prefers-reduced-motion: reduce)");
  const steps = [
    {
      target: "#walletSetup",
      title: "1. 先准备钱包与授权",
      copy: "填写钱包名称、资金账户地址和已授权的 API 钱包私钥。资金账户是实际持仓与交易的账户，私钥来自独立的 API 钱包。",
      detail:
        "新保存的钱包使用 Windows DPAPI 加密写入本机 wallet.local.json，仅当前 Windows 用户可解密；旧明文文件在重新保存后升级。保存不完成交易所授权，也不会启动交易。不要填写或分享主钱包私钥。教程不会读取或填写输入框。",
    },
    {
      target: ".overview-budget",
      title: "2. 核对下次启动预算",
      copy: "先选市场，再设置做市单侧总额、最大仓位、最大挂单和会话回撤。四项对该市场的每个启用钱包分别生效，不是共享一份总额。",
      detail:
        "“保存固定预算”会关闭该市场按权益放大，下次启动生效。单侧总额是做市整条单侧梯子的金额；另外三项也限制网格。下方运行合计是另一种只读口径。",
    },
    {
      target: "#strategyControl",
      title: "3. 理解两种策略",
      copy: "双边做市在参考价格两侧报价；库存网格围绕锚点分层建仓与减仓。库存网格为实验性功能，尚未经过实盘测试。两种策略都可能积累仓位。",
      detail:
        "做市启动全部启用钱包的全部启用市场；网格只启动选定钱包与市场。网格方向、层数、间距、每单金额和锚点的解释都在新手指南中；教程不会替你切换策略。",
    },
    {
      target: "#startBtn",
      title: "4. 启动按钮会发送真实订单",
      copy: "先核对资金账户、市场、当前仓位、挂单和参数，再自行决定是否启动。做市的启动范围不由“查看运行”或顶部预算市场选择决定。",
      detail:
        "启动请求提交后，还要看账户授权、参数核验和报价状态。本步骤只高亮按钮，不能点穿，也不会提交启动请求。",
    },
    {
      target: ".profile-selection",
      title: "5. 选择要查看的运行记录",
      copy: "这里切换钱包、市场、模拟／实盘与策略记录，只改变查看范围，不切换后台交易范围。",
      detail: "核对所选记录、时间与状态。历史会话即使已停止，仍会显示当时的成交与盈亏。",
    },
    {
      target: "#cards",
      title: "6. 分清当前账户与历史统计",
      copy: "账户快照反映最近一次官方读取；策略历史统计描述所选运行记录。先看各自的更新时间与“历史”标签。",
      detail:
        "“—”或“数据不可用”表示尚不能确认，不等于零。持仓与挂单敞口用于观察方向风险，不能用历史盈亏替代当前账户核对。",
    },
    {
      target: ".charts-column",
      title: "7. 图表不是账户权益曲线",
      copy: "图表按所选会话保存的成交绘制。累计成交额不是收益；已实现减手续费只扣实际费用，不包含浮盈亏、资金费和返佣。",
      detail: "买卖分布按成交金额计算，不是当前仓位比例。缺失数据保持未知；投影返佣不代表到账。",
    },
    {
      target: ".records-layer",
      title: "8. 用明细核对实际行为",
      copy: "这里查看策略挂单、交易所最近 24 小时成交、脚本会话逐笔成交与运行日志。它们的时间范围和来源不同，数字不必相同。",
      detail:
        "遇到错误先看具体钱包、市场和原始时间。UNKNOWN 需要继续核对，不能当成失败后直接重发；网格配对收益也不能再次加进真实盈亏。",
    },
    {
      target: "#stopBtn",
      title: "9. 停止后仍要核对仓位",
      copy: "“全部安全撤单并停止”会请求所有策略停止和撤单。它不等于立即撤完，也不等于把已有仓位平掉。",
      detail:
        "等待停止与撤单结果，再到交易所核对仓位和挂单。关闭浏览器不会停止后台机器人。完成后遮罩会移除，原有输入和配置保持不变。",
    },
  ];
  let current = 0,
    active = false,
    returnFocus = null,
    frame = 0,
    resizeObserver = null,
    mutationObserver = null,
    observed = null;
  const remember = (status) => {
    try {
      window.localStorage.setItem(storageKey, status);
    } catch {}
  };
  const dismissWelcome = () => {
    welcome.hidden = true;
    remember("dismissed");
  };
  try {
    welcome.hidden = ["dismissed", "completed"].includes(window.localStorage.getItem(storageKey));
  } catch {
    welcome.hidden = false;
  }
  function captureFocus() {
    if (!center.open && !tour.open) returnFocus = document.activeElement;
  }
  function restoreFocus() {
    const target =
      returnFocus &&
      returnFocus.isConnected &&
      !returnFocus.disabled &&
      returnFocus.getClientRects().length &&
      typeof returnFocus.focus === "function"
        ? returnFocus
        : entry;
    target.focus({ preventScroll: true });
    returnFocus = null;
  }
  function finish(completed = false) {
    active = false;
    if (frame) {
      cancelAnimationFrame(frame);
      frame = 0;
    }
    resizeObserver?.disconnect();
    mutationObserver?.disconnect();
    observed = null;
    spot.hidden = true;
    tour.classList.remove("guide-missing");
    if (tour.open) tour.close();
    if (center.open) center.close();
    if (completed) remember("completed");
    restoreFocus();
  }
  function openCenter() {
    captureFocus();
    if (tour.open) {
      active = false;
      resizeObserver?.disconnect();
      mutationObserver?.disconnect();
      spot.hidden = true;
      tour.close();
    }
    dismissWelcome();
    if (!center.open) center.showModal();
    get("guideTitle").focus({ preventScroll: true });
  }
  function focusTrap(event, dialog) {
    if (event.key !== "Tab") return;
    const nodes = [
      ...dialog.querySelectorAll('button:not([disabled]),a[href],[tabindex]:not([tabindex="-1"])'),
    ].filter((node) => node.getClientRects().length && !node.hidden);
    if (!nodes.length) {
      event.preventDefault();
      return;
    }
    const first = nodes[0],
      last = nodes[nodes.length - 1],
      focused = document.activeElement;
    if (event.shiftKey && (focused === first || !nodes.includes(focused))) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && (focused === last || !nodes.includes(focused))) {
      event.preventDefault();
      first.focus();
    }
  }
  function queuePosition() {
    if (active && !frame)
      frame = requestAnimationFrame(() => {
        frame = 0;
        positionStep();
      });
  }
  const clamp = (n, lo, hi) => Math.max(lo, Math.min(n, Math.max(lo, hi)));
  function positionStep() {
    if (!active || !tour.open) return;
    const target = document.querySelector(steps[current].target),
      vw = document.documentElement.clientWidth || window.innerWidth,
      vh = window.innerHeight,
      margin = 12,
      gap = 14;
    card.style.width = Math.min(380, vw - 24) + "px";
    card.style.maxHeight = vh - 24 + "px";
    const note = get("guideTargetNote");
    note.hidden = false;
    note.textContent = "";
    const cw = card.offsetWidth,
      ch = card.offsetHeight;
    if (!target || !target.isConnected || !target.getClientRects().length) {
      spot.hidden = true;
      tour.classList.add("guide-missing");
      card.style.left = clamp((vw - cw) / 2, margin, vw - cw - margin) + "px";
      card.style.top = clamp((vh - ch) / 2, margin, vh - ch - margin) + "px";
      note.hidden = false;
      note.textContent = "当前区域暂不可见，可继续下一步；教程不会改变策略。";
      return;
    }
    tour.classList.remove("guide-missing");
    if (observed !== target) {
      resizeObserver?.disconnect();
      resizeObserver?.observe(target);
      observed = target;
    }
    const r = target.getBoundingClientRect();
    let left,
      top,
      vertical = false;
    if (vw > 760 && r.right + gap + cw <= vw - margin) {
      left = r.right + gap;
      top = clamp(r.top, margin, vh - ch - margin);
    } else if (vw > 760 && r.left - gap - cw >= margin) {
      left = r.left - gap - cw;
      top = clamp(r.top, margin, vh - ch - margin);
    } else {
      vertical = true;
      left = clamp((vw - cw) / 2, margin, vw - cw - margin);
      top = Math.max(margin, vh - ch - margin);
    }
    card.style.left = left + "px";
    card.style.top = top + "px";
    const x = Math.max(margin, r.left - 4),
      y = Math.max(margin, r.top - 4),
      right = Math.min(vw - margin, r.right + 4),
      bottom = Math.min(vertical ? top - gap : vh - margin, r.bottom + 4);
    if (right <= x || bottom <= y) {
      spot.hidden = true;
      tour.classList.add("guide-missing");
      note.hidden = false;
      note.textContent = "正在定位此区域；也可继续阅读下一步。";
      return;
    }
    spot.hidden = false;
    Object.assign(spot.style, {
      left: x + "px",
      top: y + "px",
      width: right - x + "px",
      height: bottom - y + "px",
    });
    const clipped = r.height > bottom - y + 10;
    note.textContent = clipped ? "此区域较长，仅高亮可见部分；关闭后可完整查看。" : "";
  }
  function showStep(index) {
    current = clamp(index, 0, steps.length - 1);
    const step = steps[current];
    get("guideStepCount").textContent = `第 ${current + 1} / ${steps.length} 步`;
    get("guideProgress").setAttribute("aria-valuenow", String(current + 1));
    get("guideProgressBar").style.width = ((current + 1) / steps.length) * 100 + "%";
    get("guideStepTitle").textContent = step.title;
    get("guideStepCopy").textContent = step.copy;
    get("guideStepDetail").textContent = step.detail;
    get("guidePrevious").disabled = current === 0;
    get("guideNext").textContent = current === steps.length - 1 ? "完成引导" : "下一步";
    const target = document.querySelector(step.target);
    positionStep();
    if (target && target.getClientRects().length) {
      const rect = target.getBoundingClientRect();
      window.scrollTo({
        top: Math.max(0, window.scrollY + rect.top - 20),
        behavior: motion.matches ? "instant" : "smooth",
      });
    }
    get("guideStepTitle").focus({ preventScroll: true });
    if (!motion.matches && typeof card.animate === "function")
      card.animate(
        [
          { opacity: 0.5, transform: "translateY(4px)" },
          { opacity: 1, transform: "translateY(0)" },
        ],
        { duration: 170, easing: "ease-out" },
      );
    queuePosition();
  }
  function startTour(index = 0) {
    captureFocus();
    dismissWelcome();
    if (center.open) center.close();
    if (!tour.open) tour.showModal();
    active = true;
    if (typeof ResizeObserver === "function") resizeObserver ??= new ResizeObserver(queuePosition);
    if (typeof MutationObserver === "function") {
      mutationObserver ??= new MutationObserver(queuePosition);
      mutationObserver.observe(document.querySelector("main"), {
        childList: true,
        subtree: true,
      });
    }
    showStep(index);
  }
  entry.addEventListener("click", openCenter);
  get("guideWelcomeOpen").addEventListener("click", openCenter);
  get("guideWelcomeDismiss").addEventListener("click", dismissWelcome);
  get("guideClose").addEventListener("click", () => finish());
  get("guideStartTour").addEventListener("click", () => startTour());
  get("guideTourClose").addEventListener("click", () => finish());
  get("guideSkip").addEventListener("click", () => finish());
  get("guidePrevious").addEventListener("click", () => showStep(current - 1));
  get("guideNext").addEventListener("click", () =>
    current === steps.length - 1 ? finish(true) : showStep(current + 1),
  );
  center.querySelectorAll("[data-guide-topic]").forEach((button) =>
    button.addEventListener("click", () =>
      get(button.dataset.guideTopic)?.scrollIntoView({
        block: "start",
        behavior: motion.matches ? "instant" : "smooth",
      }),
    ),
  );
  center
    .querySelectorAll("[data-guide-jump]")
    .forEach((button) =>
      button.addEventListener("click", () => startTour(Number(button.dataset.guideJump))),
    );
  for (const dialog of [center, tour]) {
    dialog.addEventListener("cancel", (event) => {
      event.preventDefault();
      finish();
    });
    dialog.addEventListener("keydown", (event) => {
      focusTrap(event, dialog);
      if (dialog === tour && (event.key === "ArrowRight" || event.key === "ArrowLeft")) {
        event.preventDefault();
        if (event.key === "ArrowRight" && current < steps.length - 1) showStep(current + 1);
        else if (event.key === "ArrowLeft" && current > 0) showStep(current - 1);
      }
    });
  }
  tour.addEventListener(
    "wheel",
    (event) => {
      if (!card.contains(event.target)) event.preventDefault();
    },
    { passive: false },
  );
  tour.addEventListener(
    "touchmove",
    (event) => {
      if (!card.contains(event.target)) event.preventDefault();
    },
    { passive: false },
  );
  window.addEventListener("scroll", queuePosition, { passive: true });
  window.addEventListener("resize", queuePosition);
  window.visualViewport?.addEventListener("resize", queuePosition);
  window.EntropyGuide = Object.freeze({
    open: openCenter,
    start: startTour,
    close: () => finish(),
  });
})();
