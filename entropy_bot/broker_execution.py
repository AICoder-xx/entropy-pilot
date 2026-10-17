"""Signer initialization, execution serialization and action budgets."""
from __future__ import annotations

import asyncio
import time
import logging

from .broker_protocol import PaidActionRecoveryUnavailable
from .signing_lock import SignerProcessLock, SigningIdentityUnavailable

log = logging.getLogger("entropy.broker")

class LiveExecutionMixin:
    """Signer initialization, execution serialization and action budgets. Shared state is initialized only by LiveBroker."""

    async def start(self) -> None:
        if self.cfg.runtime.auto_reserve_actions:
            raise PaidActionRecoveryUnavailable()
        try:
            from eth_account import Account
            from hyperliquid.exchange import Exchange
        except ImportError as exc:
            raise RuntimeError(
                "实盘需要安装 requirements.txt（官方 hyperliquid-python-sdk）"
            ) from exc
        wallet = Account.from_key(self.private_key)
        if wallet.address.lower() == self.address:
            account_role, account_abstraction = await asyncio.gather(
                self.market.user_role(self.address),
                self.market.user_abstraction(self.address),
            )
            signer_role = account_role
        else:
            signer_role, account_role, account_abstraction = await asyncio.gather(
                self.market.user_role(wallet.address),
                self.market.user_role(self.address),
                self.market.user_abstraction(self.address),
            )
        self.account_abstraction = str(account_abstraction or "default")
        if self.account_abstraction == "portfolioMargin":
            raise RuntimeError(
                "当前为 portfolioMargin；本版本尚未实现跨资产组合保证金精确核算，拒绝实盘"
            )
        target_role = account_role.get("role")
        if target_role == "user":
            target_master = self.address
        elif target_role == "subAccount":
            target_master = str(
                (account_role.get("data") or {}).get("master") or ""
            ).lower()
        else:
            raise RuntimeError(
                f"HL_ACCOUNT_ADDRESS 不是主账户或子账户: {target_role or 'missing'}"
            )

        signer_kind = signer_role.get("role")
        signer_address = wallet.address.lower()
        if signer_kind == "user":
            if signer_address != target_master:
                raise RuntimeError(
                    "主钱包私钥与目标主账户/子账户所属主账户不匹配"
                )
            signer_label = "main-wallet"
        elif signer_kind == "agent":
            master = str(
                (signer_role.get("data") or {}).get("user") or ""
            ).lower()
            if not master or master != target_master:
                raise RuntimeError("API Wallet 未被这个主账户/子账户所属主账户授权")
            signer_label = "api-wallet"
        else:
            raise RuntimeError(
                f"签名钱包不是可用的主钱包或 API Wallet: {signer_kind or 'missing'}"
            )
        # Official address limits treat subaccounts as separate users. Poll
        # the actual trading account, not its signing/authorization master.
        self._rate_limit_address = self.address
        await self._assert_recent_external_activity_quiet()
        await self._assert_no_foreign_open_orders_before_mutation()
        # The official constructor fetches spotMeta, perpDexs and meta with
        # synchronous requests. Keep those reads off the event-loop thread;
        # signed actions still use the existing async/process mutex below.
        self.exchange = await asyncio.to_thread(
            Exchange,
            wallet,
            base_url=self.cfg.market.api_url,
            vault_address=self.address if target_role == "subAccount" else None,
            account_address=self.address,
            perp_dexs=[self.cfg.market.dex],
            timeout=10.0,
        )
        self._signing_lock = SignerProcessLock(wallet.address)
        await self._ensure_target_leverage()
        log.info(
            "实盘签名器已加载: type=%s signer=%s account=%s abstraction=%s",
            signer_label,
            wallet.address[:6] + "..." + wallet.address[-4:],
            self.address[:6] + "..." + self.address[-4:],
            self.account_abstraction,
        )

    async def _ensure_target_leverage(self) -> None:
        target = self.cfg.market.target_leverage
        if target is None:
            return
        if target > self.meta.max_leverage:
            raise RuntimeError(
                f"目标杠杆 {target}x 超过 {self.meta.coin} 上限 "
                f"{self.meta.max_leverage}x"
            )

        async def current() -> tuple[str, int, dict]:
            data = await self.market.active_asset_data(self.address)
            self._update_official_leverage(data)
            leverage = data.get("leverage") or {}
            value = self.official_leverage_value or 0
            return str(leverage.get("type") or ""), value, data

        kind, value, _ = await current()
        if kind == "isolated" and value == target:
            log.info("逐仓杠杆已核验: %s %dx", self.meta.coin, target)
            return
        assert self.exchange is not None

        async def submit_leverage() -> dict:
            try:
                result = await self._sdk(
                    self.exchange.update_leverage,
                    target,
                    self.meta.coin,
                    False,
                )
            except Exception as exc:
                # Reconcile official state once; never blindly submit it again.
                await asyncio.sleep(0.5)
                now_kind, now_value, _ = await current()
                if now_kind == "isolated" and now_value == target:
                    log.info(
                        "杠杆提交响应不确定，但官方状态已确认: %s %dx",
                        self.meta.coin,
                        target,
                    )
                    return {"status": "ok", "reconciled": True}
                raise RuntimeError("杠杆更新结果不确定，已停止且不重试") from exc
            return result if isinstance(result, dict) else {"status": "invalid", "response": result}

        async def add_required_margin() -> None:
            state, active = await asyncio.gather(
                self.market.account_state(self.address),
                self.market.active_asset_data(self.address),
            )
            position = next(
                (
                    (item.get("position") or {})
                    for item in state.get("assetPositions") or []
                    if (item.get("position") or {}).get("coin") == self.meta.coin
                ),
                None,
            )
            if not position:
                raise RuntimeError("杠杆下降需要追加保证金，但官方未返回目标仓位")
            position_value = abs(float(position.get("positionValue") or 0.0))
            margin_used = float(position.get("marginUsed") or 0.0)
            required = position_value / target
            amount = round(max(required - margin_used, 0.0) + 1.0, 6)
            available = [float(x) for x in (active.get("availableToTrade") or [])]
            free = min(available) if len(available) == 2 else 0.0
            if amount <= 0 or amount > self.cfg.risk.capital_usd * 0.2:
                raise RuntimeError(f"自动追加逐仓保证金金额异常: ${amount:.2f}")
            if free - amount < self.cfg.risk.min_free_collateral_usd:
                raise RuntimeError("追加逐仓保证金后将低于可用 USDC 保留线")
            before_raw = float(((position.get("leverage") or {}).get("rawUsd")) or 0.0)
            try:
                result = await self._sdk(
                    self.exchange.update_isolated_margin,
                    amount,
                    self.meta.coin,
                )
            except Exception as exc:
                await asyncio.sleep(0.5)
                after_state = await self.market.account_state(self.address)
                after_position = next(
                    (
                        (item.get("position") or {})
                        for item in after_state.get("assetPositions") or []
                        if (item.get("position") or {}).get("coin") == self.meta.coin
                    ),
                    {},
                )
                after_raw = float(
                    ((after_position.get("leverage") or {}).get("rawUsd")) or 0.0
                )
                if after_raw >= before_raw + amount - 0.02:
                    log.info(
                        "追加保证金响应不确定，但官方状态已确认: %s $%.2f",
                        self.meta.coin,
                        amount,
                    )
                    return
                raise RuntimeError("追加逐仓保证金结果不确定，已停止且不重试") from exc
            if not isinstance(result, dict) or result.get("status") != "ok":
                raise RuntimeError(f"追加逐仓保证金失败: {str(result)[:240]}")
            log.info("已为 %s 追加逐仓保证金 $%.2f", self.meta.coin, amount)
            await asyncio.sleep(0.5)

        response = await submit_leverage()
        error_text = str(response.get("response") or "")
        if response.get("status") != "ok" and "sufficient margin" in error_text.lower():
            await add_required_margin()
            response = await submit_leverage()
        if response.get("status") != "ok":
            raise RuntimeError(f"杠杆更新失败: {str(response)[:240]}")
        kind, value, _ = await current()
        if kind != "isolated" or value != target:
            raise RuntimeError(
                f"杠杆写入后官方状态不一致: {kind or 'unknown'} {value}x"
            )
        log.info("逐仓杠杆已设置并核验: %s %dx", self.meta.coin, target)

    def _sdk_with_process_lock(self, fn, *args):
        if self._signing_lock is None:
            raise SigningIdentityUnavailable("签名身份尚未初始化；拒绝调用签名器")
        return self._signing_lock.run(fn, *args)

    async def _sdk(self, fn, *args):
        async with self._lock:
            # Cancelling to_thread only cancels its awaiter, not its running
            # thread. Retain both serialization layers until that call settles.
            worker = asyncio.create_task(
                asyncio.to_thread(self._sdk_with_process_lock, fn, *args)
            )
            cancelled = False
            while True:
                try:
                    result = await asyncio.shield(worker)
                    break
                except asyncio.CancelledError:
                    cancelled = True
                    if worker.done():
                        break
                    # Repeated cancellation must not release the lock early.
                except Exception:
                    if cancelled:
                        raise asyncio.CancelledError() from None
                    raise
            if cancelled:
                # Retrieve a possible exception to avoid an unobserved worker
                # error. The caller reserves its identity before propagating.
                if not worker.cancelled():
                    worker.exception()
                raise asyncio.CancelledError()
            return result

    async def refresh_action_budget(self) -> None:
        """Poll the official, read-only action budget on its own clock."""
        now = time.monotonic()
        if now < self._budget_next_poll:
            return
        self._budget_next_poll = now + self.cfg.runtime.budget_poll_sec
        payload = await self.market.user_rate_limit(self._rate_limit_address)
        self.action_budget.apply_official(payload)
        if self.action_budget.state in {"soft", "hard"}:
            log.warning(
                "地址动作额度吃紧: remaining≈%.0f cap=%.0f (cumVlm=%.0f)",
                self.action_budget.remaining,
                self.action_budget.cap,
                self.action_budget.cum_volume,
            )
            await self._maybe_reserve_actions()

    async def _maybe_reserve_actions(self) -> None:
        if self.cfg.runtime.auto_reserve_actions:
            raise PaidActionRecoveryUnavailable()

    def _reserve_weight_sync(self, weight: int):
        """Fail closed until payment intent/reconciliation is persistent."""
        raise PaidActionRecoveryUnavailable()
