from __future__ import annotations

import json
import os
import re
import shutil
from datetime import datetime
from datetime import timezone
from html import escape
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


def build_report_html(cfg: dict, metric_map: dict[str, float], image_rel_path: str) -> str:
    title = escape(str(cfg["report"]["title"]))
    rows = [
        ("区间总收益", format_pct(metric_map.get("total_return", 0.0))),
        ("年化收益", format_pct(metric_map.get("annualized_return", 0.0))),
        ("年化波动", format_pct(metric_map.get("annualized_volatility", 0.0))),
        ("Sharpe", f"{metric_map.get('sharpe', 0.0):.4f}"),
        ("当前回撤", format_pct(metric_map.get("drawdown", 0.0))),
        ("最大回撤", format_pct(metric_map.get("max_drawdown", 0.0))),
        ("平均换手", format_pct(metric_map.get("average_turnover", 0.0))),
        ("成本占比", format_pct(metric_map.get("cost_ratio", 0.0))),
        ("调仓次数", str(int(metric_map.get("total_periods", 0.0)))),
    ]

    metric_rows = "\n".join(
        f"<tr><th>{escape(name)}</th><td>{escape(value)}</td></tr>" for name, value in rows
    )
    image_path = escape(image_rel_path)
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 32px auto; max-width: 1000px; line-height: 1.65; color: #1f2937; padding: 0 18px; }}
    h1, h2 {{ color: #111827; }}
    .card {{ border: 1px solid #e5e7eb; border-radius: 10px; padding: 16px; margin-bottom: 20px; background: #ffffff; }}
    .actions a {{ display: inline-block; padding: 8px 12px; background: #2563eb; color: #fff; text-decoration: none; border-radius: 6px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border-bottom: 1px solid #e5e7eb; padding: 10px 8px; text-align: left; }}
    th {{ width: 180px; color: #374151; font-weight: 600; }}
    img {{ max-width: 100%; border: 1px solid #e5e7eb; border-radius: 8px; }}
    code {{ background: #f3f4f6; padding: 2px 6px; border-radius: 4px; }}
  </style>
</head>
<body>
  <h1>{title}</h1>
  <div class="card">
    <p class="actions"><a href="../../index.html">返回历史首页</a></p>
  </div>
  <div class="card">
    <h2>策略定义</h2>
    <ul>
      <li>股票池：沪深300（当前为 mock 占位）</li>
      <li>信号：<code>score = 0.5 * mom60 + 0.5 * mom120</code>（t 日收盘计算）</li>
      <li>调仓：每周一次，t+1 开盘成交</li>
      <li>成本：buy=0.0008, sell=0.0018</li>
    </ul>
  </div>
  <div class="card">
    <h2>回测指标</h2>
    <table>
      {metric_rows}
    </table>
  </div>
  <div class="card">
    <h2>净值曲线</h2>
    <img src="{image_path}" alt="净值曲线" />
  </div>
  <div class="card">
    <h2>未来函数检查</h2>
    <ul>
      <li>信号日期：t（收盘后）</li>
      <li>交易日期：t+1（下一交易日开盘）</li>
      <li>回测引擎已强制按上述顺序执行</li>
    </ul>
  </div>
</body>
</html>
"""


def _sanitize_report_id(raw_value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9._-]", "-", raw_value).strip("-")
    return safe[:80]


def _resolve_report_id() -> str:
    report_id = _sanitize_report_id(os.getenv("REPORT_ID", ""))
    if report_id:
        return report_id
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S-%f")


def _copy_assets(source_dir: Path, target_dir: Path) -> list[str]:
    copied_asset_names: list[str] = []
    for source in sorted(source_dir.glob("*")):
        if source.is_file() and source.suffix.lower() in {".png", ".jpg", ".jpeg", ".gif", ".svg"}:
            target = target_dir / source.name
            shutil.copy2(source, target)
            copied_asset_names.append(source.name)
    return copied_asset_names


def _load_history(site_dir: Path) -> list[dict]:
    history_path = site_dir / "history.json"
    if not history_path.exists():
        return []
    payload = json.loads(history_path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        reports = payload.get("reports", [])
    elif isinstance(payload, list):
        reports = payload
    else:
        reports = []
    if not isinstance(reports, list):
        return []
    return [item for item in reports if isinstance(item, dict)]


def _save_history(site_dir: Path, history: list[dict]) -> Path:
    history_path = site_dir / "history.json"
    payload = {
        "latest": history[0]["id"] if history else "",
        "reports": history,
    }
    history_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return history_path


def _build_root_index(cfg: dict, history: list[dict]) -> str:
    title = escape(str(cfg["report"]["title"]))
    if not history:
        return f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><title>{title}</title></head>
<body><h1>{title}</h1><p>暂无历史报告。</p></body></html>"""

    latest = history[0]
    latest_path = escape(str(latest.get("path", "")))
    rows = []
    for item in history:
        path = escape(str(item.get("path", "")))
        report_id = escape(str(item.get("id", "")))
        generated_at = escape(str(item.get("generated_at", "")))
        commit = escape(str(item.get("commit", "")))
        commit_text = commit[:10] if commit else "-"
        rows.append(
            f"<tr><td><a href=\"{path}\">{report_id}</a></td><td>{generated_at}</td><td>{commit_text}</td></tr>"
        )
    table_rows = "\n".join(rows)
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title} - 历史报告</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 32px auto; max-width: 1000px; line-height: 1.65; color: #1f2937; padding: 0 18px; }}
    h1, h2 {{ color: #111827; }}
    .card {{ border: 1px solid #e5e7eb; border-radius: 10px; padding: 16px; margin-bottom: 20px; background: #ffffff; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border-bottom: 1px solid #e5e7eb; padding: 10px 8px; text-align: left; }}
    .actions a {{ display: inline-block; padding: 8px 12px; background: #2563eb; color: #fff; text-decoration: none; border-radius: 6px; }}
  </style>
</head>
<body>
  <h1>{title} - 历史报告</h1>
  <div class="card">
    <h2>最新报告</h2>
    <p>将于 2 秒后自动跳转到最新报告；若未跳转，请手动点击按钮。</p>
    <p class="actions"><a href="{latest_path}">打开最新报告</a></p>
  </div>
  <div class="card">
    <h2>历史列表</h2>
    <table>
      <thead><tr><th>报告 ID</th><th>生成时间(UTC)</th><th>Commit</th></tr></thead>
      <tbody>
      {table_rows}
      </tbody>
    </table>
  </div>
  <script>
    setTimeout(function () {{
      window.location.href = "{latest_path}";
    }}, 2000);
  </script>
</body>
</html>
"""


def build_site(report_dir: Path, site_dir: Path, metric_map: dict[str, float], cfg: dict) -> tuple[Path, Path, Path]:
    report_id = _resolve_report_id()
    report_site_dir = ensure_dir(site_dir / "reports" / report_id)
    report_assets_dir = ensure_dir(report_site_dir / "assets")

    copied_asset_names = _copy_assets(report_dir, report_assets_dir)
    if not copied_asset_names:
        raise FileNotFoundError("No static assets found in outputs/report to publish.")

    latest_assets_dir = site_dir / "assets"
    if latest_assets_dir.exists():
        shutil.rmtree(latest_assets_dir)
    latest_assets_dir = ensure_dir(latest_assets_dir)
    _copy_assets(report_dir, latest_assets_dir)

    preferred_name = "nav_curve.png" if "nav_curve.png" in copied_asset_names else copied_asset_names[0]
    html_text = build_report_html(cfg, metric_map, f"assets/{preferred_name}")
    report_index_path = report_site_dir / "index.html"
    report_index_path.write_text(html_text, encoding="utf-8")

    history = _load_history(site_dir)
    history = [item for item in history if str(item.get("id", "")) != report_id]
    history.append(
        {
            "id": report_id,
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "commit": os.getenv("GITHUB_SHA", ""),
            "path": f"reports/{report_id}/index.html",
            "assets": copied_asset_names,
        }
    )
    history.sort(key=lambda item: str(item.get("generated_at", "")), reverse=True)

    history_path = _save_history(site_dir, history)
    root_index_text = _build_root_index(cfg, history)
    site_index_path = site_dir / "index.html"
    site_index_path.write_text(root_index_text, encoding="utf-8")
    return report_index_path, site_index_path, history_path


def main() -> None:
    cfg = load_config("config.yaml")
    result_dir = Path(cfg["backtest"]["result_dir"])
    report_dir = ensure_dir(cfg["report"]["report_dir"])
    site_dir = ensure_dir("outputs/site")

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
    site_report_path, site_index_path, site_history_path = build_site(
        report_dir, site_dir, metric_map, cfg
    )

    print(f"[report] figure={fig_path}")
    print(f"[report] markdown={report_path}")
    print(f"[report] site_latest_assets={site_dir / 'assets'}")
    print(f"[report] site_report={site_report_path}")
    print(f"[report] site_index={site_index_path}")
    print(f"[report] site_history={site_history_path}")
    print("[report] done")


if __name__ == "__main__":
    main()
