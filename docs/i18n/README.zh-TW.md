# Entropy Pilot

**Entropy 雙邊做市、真實庫存網格與本機瀏覽器控制台。**

[简体中文](../../README.md) · [English](README.en.md) · 繁體中文 · [日本語](README.ja.md) · [한국어](README.ko.md)

Entropy Pilot 在自己的電腦執行，支援 `io:SNDK` 與 `io:ANTH`，整合錢包設定、風險預算、策略控制、圖表與日誌。**目前控制台介面為簡體中文；本頁是文件翻譯，不代表介面已支援語言切換。**

> 控制台的「啟動實盤」按鈕會送出真實訂單。公開命令列設定預設為模擬盤。做市、網格及邀請返佣都不保證獲利。

> **庫存網格為實驗性功能，尚未經過實盤測試，目前僅完成程式碼層面的實作與測試；不代表實盤可用性已經驗證。**

## 核心功能

- **雙邊做市：** ALO/Maker 買賣報價、庫存偏移、倉位及掛單限制。
- **真實庫存網格（實驗性）：** 支援做多或做空；平倉單只減少已有庫存，配對收益與帳戶損益分開記錄。僅完成程式碼實作與測試，尚未經過實盤驗證。
- **多錢包、多市場：** 執行實例與訂單、成交、恢復記錄分離。
- **本機控制台：** 填寫錢包、儲存固定預算、啟動與安全停止、檢視圖表及日誌。
- **結果不確定時先對帳：** 保留 `UNKNOWN`，核對原訂單後再決定下一步。
- **9 步新手引導：** 高亮說明相關區域，不代替使用者儲存設定或送單。

## Windows 快速開始

安裝 [Git](https://git-scm.com/downloads) 與 [Python 3.11 以上版本](https://www.python.org/downloads/)，保留 Windows Python Launcher（`py`）。本次安裝、測試及瀏覽器操作以 **Windows / Python 3.13** 驗證。模擬盤不需要私鑰；實盤需要自行準備交易所資金帳戶及獨立授權的 API 錢包。

也可在 PowerShell 安裝 Git 與 Python 3.13：

```powershell
winget install --id Git.Git -e --source winget
winget install --id Python.Python.3.13 -e --source winget
```

若沒有 `winget`，請使用上方 Git 與 Python 官方下載連結安裝。完成後**關閉並重新開啟 PowerShell**，確認版本並設定專案：

```powershell
git --version
py -3.13 --version
git clone https://github.com/AICoder-xx/entropy-pilot.git
cd entropy-pilot
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe dashboard.py
```

入口為 [http://127.0.0.1:8765/](http://127.0.0.1:8765/)。直接呼叫虛擬環境的 Python，**不需要啟用 `Activate.ps1` 或變更 PowerShell 執行原則**。也可下載並解壓縮原始碼，雙擊 `install.bat` 安裝，再雙擊 `run_dashboard.bat`。

請透過 `run_dashboard.bat` 或 `dashboard.py` 啟動後端，再開啟上述網址。直接雙擊 `dashboard/index.html` 無法連接後端；這不是唯讀鎖定，請改由正常入口啟動。

一般模式可儲存設定。全新安裝預設停止；**已有執行記錄的目錄可能依持久狀態自動恢復**，請先確認實際狀態。關閉瀏覽器分頁不會停止服務或機器人。

## 第一次使用：9 個步驟

1. **先看指南與狀態。** 點選 `新手指南` → `带我走一遍`。引導只說明操作，不會交易。確認是否為唯讀模式，以及有沒有正在執行或恢復的策略。
2. **授權獨立 API 錢包。** 在交易所官方頁面為資金帳戶授權。不要填入主錢包私鑰；API 錢包的交易權限仍可能造成資金損失。
3. **填寫錢包資料。** 在 `钱包与配置` 輸入錢包名稱、資金帳戶地址與 API 錢包私鑰。資金帳戶是實際交易的主帳戶或已授權子帳戶，**不是 API 錢包地址**。
4. **儲存在本機。** 點選 `保存本机配置`。同名錢包會更新；已存地址或私鑰留白時保留原值。不同資金帳戶請使用新名稱。儲存不代表交易所授權已通過，啟動時仍會驗證。
5. **設定下次啟動預算。** 在頂部 `风险预算` 選擇 SNDK 或 ANTH，填入做市單側總額、最大倉位、最大掛單及會話回撤。`保存固定预算` 會關閉該市場依權益自動放大。執行或恢復中必須先安全停止才能儲存。
6. **確認策略與啟動範圍。** 雙邊做市會啟動**所有啟用錢包的全部已設定市場**，目前為 SNDK 與 ANTH；預算市場選單不是啟動範圍開關。網格只啟動選定錢包與市場，方向、層數、間距、每單金額及錨點另行設定。已有庫存或歷史記錄時不能任意變向。**庫存網格為實驗性功能，僅完成程式碼層面的實作與測試，尚未經過實盤測試；提供啟動入口不代表實盤可用性已經驗證。**
7. **核對後再決定是否實盤。** 檢查資金帳戶、實際持倉、掛單、可用保證金、預算與方向，再自行決定是否點選標示實盤的啟動按鈕。提交啟動請求不等於報價已成功。
8. **觀察成交與日誌。** 下次啟動設定和目前／上次執行統計分開顯示。遇到錯誤先看提示。`UNKNOWN` 是結果未確定，不是可直接重送的失敗。
9. **安全停止並核對剩餘倉位。** 點選 `全部安全撤单并停止`，等待確認，再到交易所檢查。**停止報價與撤單不等於平倉。** `flatten_on_halt` 只處理風控觸發時的減倉；不要用關分頁、強制結束程序或刪除帳本代替安全停止。

## 預算與設定檔

四項預算分別適用於**所選市場的每個啟用錢包**，不是多個錢包共用一份總額。做市單側總額是一側報價階梯的總額，不是每層金額；網格每單金額獨立設定，另外三項風險限制也約束該市場網格。總風險須合併考量所有錢包與市場。

公開範例每市場、每錢包採 `$12` 單側做市、`$30` 最大淨倉位、`$30` 最大掛單及 `$3` 會話回撤。這些只是方便檢查的小額範本，不是建議配置或最大虧損保證；交易所最低金額與精度可能使其不適用。預設停用自動放大、付費購買 API 動作額度，以及未到帳返佣投影。

| 檔案 | 用途 |
|---|---|
| `config.json` | 預設命令列與做市模擬設定，模式為 `paper`；不含私鑰 |
| `config.grid.json` | 網格模擬設定，模式為 `paper` |
| `config.live.example.json` | 控制台讀取的實盤範本；憑證留白，須先在本機設定錢包 |
| `dashboard.config.json` | 儲存後的本機風險預算 |
| `dashboard.config.example.json` | 沒有本機預算檔時使用的小額預設 |
| `wallet.local.json` | GUI 儲存的本機憑證，格式參考 `wallet.example.json` |
| `.env` | 可選單錢包 CLI 憑證；須由 `live.env_file` 明確指定才讀取 |
| `state/`、`logs/` | 持久訂單、成交、庫存與執行記錄 |

儲存預算立即更新下次啟動值，不會啟動交易或改寫歷史統計。手動修改其他檔案時，先安全停止，再重啟相關程序。兩份預算檔都缺失或本機檔損壞會報錯，不會隱藏回退到大額預算。

命名錢包各自使用 JSON 憑證。`primary` 的優先順序為程序環境變數、明確指定的 `.env`、本機 JSON；既有環境變數可能影響它，控制台會提示。建議使用例如 `wallet1` 等可區分的名稱。

## 模擬盤、實盤與唯讀模式

```powershell
# 僅查行情
.\.venv\Scripts\python.exe main.py --config config.json --check

# 模擬盤，不送真實訂單
.\.venv\Scripts\python.exe main.py --config config.json
.\.venv\Scripts\python.exe main.py --config config.grid.json

# 唯讀，不允許儲存、交易控制或自動恢復
.\.venv\Scripts\python.exe dashboard.py --port 8767 --read-only
```

也可雙擊 `check_market.bat`、`run_market_maker.bat`、`run_grid.bat`。模擬盤無法重現真實排隊位置、完整滑價或獲利表現。**一般控制台的啟動按鈕仍是實盤，不會因 CLI 預設 paper 而變成模擬盤。**

實盤支援仍以 Windows 為限。POSIX 鎖已實作，但 Linux/macOS 實盤與長期無人值守尚未完成驗收。

## 憑證、恢復與統計

- Windows 面板新儲存的錢包檔案採目前使用者 DPAPI 加密。舊明文仍可讀取，只有再次點選儲存才升級；`.env` 仍是明文。非 Windows 拒絕明文降級寫入，密文不能視為可跨電腦或使用者遷移。不要公開憑證或帳戶資料。簽名鎖已提供 Windows/POSIX 實作，實機驗收仍以 Windows 為限；新舊版本不可混跑。見[工程邊界](../ENGINEERING_QUALITY.md)。
- 更新前安全停止並備份自己的設定、憑證與狀態。不要匯入他人帳本，不要刪檔繞過 `UNKNOWN`、庫存衝突或延遲成交；先核對交易所原訂單與倉位。
- 網格配對收益不等於帳戶淨收益。已實現價差減手續費不包含完整資金費、浮動損益、轉帳與未到帳獎勵。30 秒 markout 是成交後價格變化，不是最終虧損；缺失資料不能當零。

```powershell
.\.venv\Scripts\python.exe analyze_efficiency.py --state-dir state --format table
.\.venv\Scripts\python.exe audit_local_data.py --output reports/local-data.json
```

## 自願使用的邀請連結

[透過 traderx 開啟 Entropy](https://entropy.io/?r=traderx)

作者確認邀請權益為 **75%，按 Entropy 收取的手續費份額計算**，不是全部交易手續費的 75%。實際資格、比例與到帳依官方帳戶頁及現行規則為準；此說明不修改交易費率，也不把未到帳返佣計入實際損益。

程式不自動簽名綁定、不變更既有邀請關係、不加入 builder 費用。邀請及自身等級權益按官方規則擇高，不疊加。

## 授權

專案採 [MIT 授權](../../LICENSE)，第三方來源與聲明見 [THIRD_PARTY_NOTICES.md](../../THIRD_PARTY_NOTICES.md)。
