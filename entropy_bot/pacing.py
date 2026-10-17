"""Address-based action budget tracking (Hyperliquid L1 rate limits).

Hyperliquid enforces a per-address action budget: roughly ``10_000 + 1
action per 1 USDC of cumulative traded volume`` (HIP-3 growth-mode volume is
credited at 10%).  Cancels get a separate, higher allowance
(``min(limit + 100_000, limit * 2)``), so risk-reducing exits stay possible
even when the base budget is exhausted.  When an address runs out entirely it
is throttled to one action per 10 seconds, which would destroy quote quality
long before it stops the bot.

``ActionBudget`` mirrors the official ``userRateLimit`` payload and layers a
local running estimate between polls so the engine can throttle *before* the
exchange does.  The official poll remains the authority: every refresh
replaces the local estimate.
"""

from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass
class ActionBudget:
    soft_remaining: float = 2500.0
    hard_remaining: float = 800.0

    cap: float = 0.0
    used_official: float = 0.0
    cum_volume: float = 0.0
    updated_mono: float = 0.0
    local_spent: float = 0.0
    # Never reset at an official poll: this is the process-session denominator
    # for volume/action efficiency, whereas local_spent estimates only the
    # unreflected actions since the latest poll.
    session_spent: float = 0.0

    def apply_official(self, payload: dict) -> None:
        """Adopt an official userRateLimit payload and reset local estimates."""

        def number(name: str) -> float:
            value = payload.get(name)
            try:
                return float(value) if value not in (None, "") else 0.0
            except (TypeError, ValueError):
                return 0.0

        self.cap = number("nRequestsCap")
        self.used_official = number("nRequestsUsed")
        self.cum_volume = number("cumVlm")
        self.updated_mono = time.monotonic()
        self.local_spent = 0.0

    def spend(self, actions: int = 1) -> None:
        """Record a locally submitted action between official polls."""
        if actions > 0:
            self.local_spent += actions
            self.session_spent += actions

    @property
    def known(self) -> bool:
        return self.updated_mono > 0

    @property
    def remaining(self) -> float:
        """Estimated remaining actions (official snapshot minus local sends).

        Fills since the last poll only add budget, so this estimate is
        conservative in the safe direction.
        """
        return self.cap - self.used_official - self.local_spent

    @property
    def state(self) -> str:
        if not self.known:
            return "unknown"
        remaining = self.remaining
        if remaining <= self.hard_remaining:
            return "hard"
        if remaining <= self.soft_remaining:
            return "soft"
        return "normal"

    def snapshot(
        self,
        *,
        session_volume_usd: float | None = None,
        growth_mode: bool | None = None,
    ) -> dict:
        volume_per_action = (
            session_volume_usd / self.session_spent
            if session_volume_usd is not None and self.session_spent > 0
            else None
        )
        target = 10.0 if growth_mode else 1.0 if growth_mode is not None else None
        return {
            "state": self.state,
            "cap": self.cap,
            "used_official": self.used_official,
            "local_spent": self.local_spent,
            "session_spent": self.session_spent,
            "remaining_est": self.remaining if self.known else None,
            "cum_volume": self.cum_volume,
            "session_volume_usd": session_volume_usd,
            "volume_per_action_usd": volume_per_action,
            "self_funding_target_usd_per_action": target,
            "self_funding_ratio": (
                volume_per_action / target
                if volume_per_action is not None and target
                else None
            ),
            "age_sec": (
                round(time.monotonic() - self.updated_mono, 1)
                if self.known
                else None
            ),
        }
