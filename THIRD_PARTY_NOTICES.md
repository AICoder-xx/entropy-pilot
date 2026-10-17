# Third-party notices

This project adapts the following ideas and small implementation patterns from
[`your-quantguy/entropy-arb`](https://github.com/your-quantguy/entropy-arb),
commit `aa0391471f6bf72f78c45801fb8117b7bf7e8c89`:

- the official Hyperliquid `l2Book` websocket subscription and reconnect loop;
- HIP-3 market discovery and asset-id calculation;
- Hyperliquid price rounding;
- client-order-id reconciliation after an uncertain order response.

The upstream project is licensed under the MIT License:

> MIT License
>
> Copyright (c) 2026 yourQuantGuy
>
> Permission is hereby granted, free of charge, to any person obtaining a copy
> of this software and associated documentation files (the "Software"), to deal
> in the Software without restriction, including without limitation the rights
> to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
> copies of the Software, and to permit persons to whom the Software is
> furnished to do so, subject to the following conditions:
>
> The above copyright notice and this permission notice shall be included in all
> copies or substantial portions of the Software.
>
> THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
> IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
> FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
> AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
> LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
> OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
> SOFTWARE.

Live signing uses the official
[`hyperliquid-python-sdk`](https://github.com/hyperliquid-dex/hyperliquid-python-sdk)
as an installed dependency.
