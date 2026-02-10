from __future__ import annotations

from pathlib import Path

from src.momentum_weekly.config_utils import ensure_dir, load_config
from src.momentum_weekly.io_utils import read_table
from src.momentum_weekly.plot_utils import save_nav_curve_png


def format_pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def build_report_md(cfg: dict, metric_map: dict[str, float], figure_path: Path) -> str:
    lines = [
        f"# {cfg['report']['title']}",
        "",
        "## 策略定义",
        "- 股票池：沪深300（当前为 mock 占位）",
        "- 信号：`score = 0.5 * mom60 + 0.5 * mom120`（t 日收盘计算）",
        "- 调仓：每周一次，t+1 开盘成交",
        "- 成本：buy=0.0008, sell=0.0018",
        "",
        "## 回测指标",
        f"- 区间总收益：{format_pct(metric_map.get('total_return', 0.0))}",
        f"- 年化收益：{format_pct(metric_map.get('annualized_return', 0.0))}",
        f"- 年化波动：{format_pct(metric_map.get('annualized_volatility', 0.0))}",
        f"- Sharpe：{metric_map.get('sharpe', 0.0):.4f}",
        f"- 当前回撤：{format_pct(metric_map.get('drawdown', 0.0))}",
        f"- 最大回撤：{format_pct(metric_map.get('max_drawdown', 0.0))}",
        f"- 平均换手：{format_pct(metric_map.get('average_turnover', 0.0))}",
        f"- 成本占比：{format_pct(metric_map.get('cost_ratio', 0.0))}",
        f"- 调仓次数：{int(metric_map.get('total_periods', 0.0))}",
        "",
        "## 净值曲线",
        f"![净值曲线]({figure_path.name})",
        "",
        "## 未来函数检查",
        "- 信号日期：t（收盘后）",
        "- 交易日期：t+1（下一交易日开盘）",
        "- 回测引擎已强制按上述顺序执行",
    ]
    return "\n".join(lines)


def main() -> None:
    cfg = load_config("config.yaml")
    result_dir = Path(cfg["backtest"]["result_dir"])
    report_dir = ensure_dir(cfg["report"]["report_dir"])

    nav_path = result_dir / "nav.parquet"
    metrics_path = result_dir / "metrics.parquet"
    if not nav_path.exists() or not metrics_path.exists():
        raise FileNotFoundError("Backtest outputs missing. Please run backtest.py first.")

    nav_rows = read_table(nav_path)
    metrics_rows = read_table(metrics_path)

    nav_rows.sort(key=lambda item: item["trade_date"])
    nav_values = [float(item["nav"]) for item in nav_rows]

    fig_path = report_dir / "nav_curve.png"
    save_nav_curve_png(fig_path, nav_values)

    metric_map = {str(item["metric"]): float(item["value"]) for item in metrics_rows}
    report_text = build_report_md(cfg, metric_map, fig_path)
    report_path = report_dir / "report.md"
    report_path.write_text(report_text, encoding="utf-8")

    print(f"[report] figure={fig_path}")
    print(f"[report] markdown={report_path}")
    print("[report] done")


if __name__ == "__main__":
    main()

