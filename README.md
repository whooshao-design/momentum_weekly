# 周调仓中期动量回测（离线最小可运行版）

这是一个 Python 3.11 的最小可运行量化回测项目，支持离线 mock 数据，完整流程如下：

1. `fetch_data.py`：获取（或生成）数据并分块写入 Parquet
2. `prepare_data.py`：整理原始数据为标准回测输入
3. `signals.py`：计算中期动量信号 `score=0.5*mom60+0.5*mom120`
4. `backtest.py`：按周调仓，t 日信号、t+1 开盘成交，扣减买卖成本
5. `report.py`：生成 `outputs/report/report.md`、净值曲线图，以及可追溯历史的 `outputs/site/`

## 目录结构

```text
.
├── src/
├── data/
├── outputs/
├── config.yaml
├── fetch_data.py
├── prepare_data.py
├── signals.py
├── backtest.py
├── report.py
├── requirements.txt
└── README.md
```

## 安装依赖

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

> 当前最小离线版仅使用标准库，`requirements.txt` 为空依赖占位。

## 运行最小示例

```bash
python fetch_data.py
python prepare_data.py
python signals.py
python backtest.py
python report.py
```

执行完成后可查看：

- `outputs/report/report.md`
- `outputs/report/nav_curve.png`
- `outputs/site/index.html`（站点入口，自动跳转最新并展示历史列表）
- `outputs/site/history.json`（历史索引）
- `outputs/site/assets/`（最新报告静态资源快照）
- `outputs/site/reports/<report_id>/index.html`（单次报告）
- `outputs/site/reports/<report_id>/assets/`（该次报告静态资源）

可通过环境变量指定报告 ID（便于 CI 追溯）：

```bash
REPORT_ID=local-20260210-1900 python report.py
```

## 核心策略定义

- 股票池：沪深300（当前使用 mock 成分占位）
- 信号：`score = 0.5 * mom60 + 0.5 * mom120`（t 日收盘）
- 调仓：每周一次（周五），成交在 t+1 交易日开盘
- 成本：买入 `0.0008`，卖出 `0.0018`
- 输出：净值、回撤、年化、波动、Sharpe、最大回撤、换手、成本占比

## 数据与扩展

- 当前默认 `provider: mock`，可离线运行。
- 数据源适配层位于 `src/momentum_weekly/data_provider.py`，已预留 `TuShareProvider` / `JoinQuantProvider` 占位实现。
- 当前 `.parquet` 文件后缀为离线 JSON fallback 存储（同接口路径），便于后续替换为真实 Parquet 引擎。

## 防未来函数说明

- `signals.py` 仅使用 t 日及以前收盘价计算信号
- `backtest.py` 强制在 `t+1` 开盘价执行交易收益计算

## GitHub Pages（发布最新并可追溯历史）

仓库已提供工作流：`.github/workflows/pages.yml`。

- 触发：`push` 到 `main`（也支持手动触发）
- 行为：
  - 构建前尝试从当前 Pages 站点恢复 `history.json` 与历史报告目录
  - 运行回测流水线并生成新报告（`REPORT_ID` 使用 GitHub run id）
  - 更新 `outputs/site/index.html`（自动跳转最新 + 历史列表）
  - 上传 `outputs/site/` 并部署到 GitHub Pages

在 GitHub 仓库页面设置：

1. 打开 `Settings` -> `Pages`
2. 在 `Build and deployment` 的 `Source` 选择 `GitHub Actions`
3. 推送到 `main` 后，等待 `Publish Report History` 工作流完成
