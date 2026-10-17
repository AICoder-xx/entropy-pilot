# 配置校验与本机凭证

## 配置组织

`config.py`负责加载与组装，`config_schema.py`定义配置模型，`config_sections.py`按市场、风险、做市、网格和运行参数解析及校验。无效值以ConfigError拒绝，保留原字段含义与预算关系检查。

## Windows钱包保存

面板使用Windows DPAPI当前用户作用域加密整个钱包文件，包括私钥、钱包标识和附加字段；不是只把私钥字段编码。加密在原子临时文件写入前完成，因此临时文件也是密文。加密、刷盘或替换失败保留旧文件，不生成新的明文备份。

旧明文钱包继续只读兼容，读取不会自动修改。Windows面板下次保存时将整个文件升级加密，保留其他钱包、禁用状态和附加字段。页面不返回私钥，只返回存储状态及可操作说明。

非Windows没有新的安全文件存储实现，创建/更新钱包时明确拒绝降级为明文；已有旧格式仍可读取。Windows密文需要原登录用户及系统密钥，不应把跨机器复制视为已完成迁移。

无法解密时保留原件，面板返回可操作的409错误。先回到保存时的用户和电脑恢复读取；不能恢复时，保留原密文，在新的独立安装/配置目录重新配置受限API钱包。不能覆盖不可读密文而丢失其他钱包。

## 保护边界

DPAPI保护静态文件，不保护已经被当前用户权限恶意程序控制的运行环境，也不能抹除用户以前自行保存的明文副本。签名期间Python对象仍持有私钥，不能承诺完整内存擦除。环境变量或用户自行配置的.env不会自动获得本钱包文件的加密保护。

依据：[CryptProtectData](https://learn.microsoft.com/en-us/windows/win32/api/dpapi/nf-dpapi-cryptprotectdata)、[CryptUnprotectData](https://learn.microsoft.com/en-us/windows/win32/api/dpapi/nf-dpapi-cryptunprotectdata)。

相关测试：`tests/test_config.py`、`tests/test_credentials.py`、`tests/test_credential_store.py`、`tests/test_dashboard_setup.py`。真实DPAPI用例只在Windows执行；跨平台的保存语义替身不被当作加密实现。
