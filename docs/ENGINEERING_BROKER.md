# 交易接口与签名互斥

`entropy_bot/broker.py` 保留公开入口和共享订单身份，订单提交、改单、撤单、IOC、账户同步、推送和签名分别由独立模块承担。账户/市场实例锁与签名锁是不同层次，不能互相替代。

## 状态约束

- 提交订单前持久化意图，首个等待点前保留整批 cloid。
- 未明确确认的订单继续保留 UNKNOWN；SDK或只读对账中断不能把它当作未提交。
- 协程取消时等待仍运行的SDK线程结束，之后才释放进程内互斥。
- IOC已到终态但成交/仓位刷新被取消时，也保留未决阻断状态。
- 已确认成功的同批委托不因另一笔失败被退回UNKNOWN。

## 签名锁

Windows使用系统字节锁，POSIX使用flock。锁按API签名钱包地址的SHA256分组，协调同一电脑、同一OS用户下的调用；不按资金账户或市场分开。原因见[官方nonce与API钱包说明](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/nonces-and-api-wallets)。

签名前先持久化进行中标记并fsync。短写、落盘失败、锁超时、异常时间状态都不进入SDK回调。进程中断或调用期间时间倒退留下SIGNING_INTERRUPTED，禁止继续签名。锁记录不包含私钥或明文账户。

锁文件位于当前用户的`~/.entropy-trader/signing-locks/`。不能在运行时删除或替换锁文件，否则不同进程可能锁住不同文件对象。它不能协调外部交易软件、其他OS用户或其他机器，请使用本应用专用API钱包。

## 升级与恢复

先正常停止旧版本并完成对账，再启动新版。新版文件锁与旧版Windows命名mutex不是同一锁域，不混跑。

遇到SIGNING_INTERRUPTED，先核对官方订单和系统时间，再在同一钱包名称下配置新的专用API钱包，保留原资金账户与账本并继续对账；不提供删除锁或自动抹除UNKNOWN的恢复通道。同步SDK线程若一直不返回，停止会等待它；强制结束会留下需人工核对的持久阻断状态。

自动付费额度续购尚无完整支付意图及UNKNOWN对账，本版拒绝启用；只读额度查询保留。

相关测试：`tests/test_signing_lock.py`、`tests/test_broker.py`、`tests/test_ws_and_modify.py`。支持范围、最新验证及升级说明见[更新日志](../CHANGELOG.md)和[质量说明](ENGINEERING_QUALITY.md)。
