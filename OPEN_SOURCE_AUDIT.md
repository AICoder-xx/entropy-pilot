# Open-source audit

Audit date: 2026-08-28

## `your-quantguy/entropy-arb`

- Repository: <https://github.com/your-quantguy/entropy-arb>
- Audited commit: `aa0391471f6bf72f78c45801fb8117b7bf7e8c89`
- License: MIT
- Strategy: two-venue taker arbitrage. Entropy is one leg; Lighter,
  Lighter Robinhood, or trade.xyz is the hedge leg.

The following upstream patterns are already adapted in this project and are
covered by `THIRD_PARTY_NOTICES.md`:

- official Hyperliquid `l2Book` websocket handling;
- HIP-3 market discovery and asset-id calculation;
- Hyperliquid price rounding;
- client-order-id reconciliation after an uncertain order response.

Upstream ideas outside this project's single-venue implementation:

- fee-aware executable-depth sizing across two books;
- premium persistence before sending both taker legs;
- immediate reduce-only repair of unmatched legs;
- minute-level cross-venue premium recording and threshold analysis.

Those features require a separately funded hedge venue. This project does not
implement cross-venue arbitrage, and these features must not be presented as risk-free.

Important differences retained in this project:

- shared weighted `/info` throttling and HTTP 429 backoff;
- no full wallet address or private key in runtime logs;
- maker-only ALO quotes for ordinary market-making/grid operation;
- official order, fill, position, and balance reconciliation before retry;
- actual equity wear, per-fill booked wear, and delayed markout wear statistics;
- browser dashboard with explicit start/stop state.

## `your-quantguy/perp-dex-tools`

- Repository: <https://github.com/your-quantguy/perp-dex-tools>
- Audited commit: `4679a339b8cdc9998707feeda3c5d8b84fb8681f`
- License: custom Non-Commercial License.

The repository demonstrates a near-price entry, take-profit close order,
inventory-dependent cooldown, and grid-spacing check. Its license expressly
prohibits use that directly or indirectly generates revenue. No source code
from that repository is copied into this project. General trading concepts
were reviewed only as an independent design comparison.

Its documented strategy also intentionally has no stop loss and can carry a
directional position while waiting for price to return. That risk model is not
used here; this project keeps position, open-order, drawdown, breakout, and
emergency controls.
