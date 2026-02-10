from __future__ import annotations

from datetime import date
from datetime import datetime
from pathlib import Path

from src.momentum_weekly.config_utils import ensure_dir, load_config
from src.momentum_weekly.io_utils import read_table, write_table


def _to_date(value: object) -> date:
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def _std(values: list[float]) -> float:
    if not values:
        return 0.0
    mean_value = sum(values) / len(values)
    variance = sum((x - mean_value) * (x - mean_value) for x in values) / len(values)
    return variance**0.5


def _sqrt(value: float) -> float:
    return value**0.5


def load_signal_rows(signal_dir: Path) -> list[dict]:
    files = sorted(signal_dir.glob("signals_chunk_*.parquet"))
    if not files:
        raise FileNotFoundError("No signal files found. Please run signals.py first.")
    rows: list[dict] = []
    for file_path in files:
        rows.extend(read_table(file_path))
    rows.sort(key=lambda item: (item["date"], item["symbol"]))
    return rows


def run_backtest(cfg: dict, signal_rows: list[dict]) -> tuple[list[dict], list[dict]]:
    top_n = int(cfg["strategy"]["top_n"])
    buy_cost = float(cfg["backtest"]["buy_cost"])
    sell_cost = float(cfg["backtest"]["sell_cost"])
    initial_nav = float(cfg["backtest"]["initial_nav"])
    trading_days_per_year = int(cfg["data"]["trading_days_per_year"])

    date_symbol_map: dict[date, dict[str, dict]] = {}
    for row in signal_rows:
        row_date = _to_date(row["date"])
        date_symbol_map.setdefault(row_date, {})[str(row["symbol"])] = row

    trading_days = sorted(date_symbol_map.keys())
    day_to_pos = {day: pos for pos, day in enumerate(trading_days)}

    rebalance_dates: list[date] = []
    for day in trading_days:
        if day.weekday() == 4 and day_to_pos[day] + 1 < len(trading_days):
            rebalance_dates.append(day)
    if len(rebalance_dates) < 2:
        raise ValueError("Not enough weekly rebalance dates to run backtest.")

    nav = initial_nav
    prev_weights: dict[str, float] = {}
    records: list[dict] = []

    for idx in range(len(rebalance_dates) - 1):
        signal_date = rebalance_dates[idx]
        next_signal_date = rebalance_dates[idx + 1]

        trade_date = trading_days[day_to_pos[signal_date] + 1]
        next_trade_date = trading_days[day_to_pos[next_signal_date] + 1]

        signal_map = date_symbol_map.get(signal_date, {})
        ranked = sorted(
            signal_map.values(),
            key=lambda item: float(item.get("score", 0.0)),
            reverse=True,
        )
        selected_symbols = [str(item["symbol"]) for item in ranked[:top_n]]
        if not selected_symbols:
            continue

        trade_map = date_symbol_map.get(trade_date, {})
        next_trade_map = date_symbol_map.get(next_trade_date, {})
        tradable_symbols: list[str] = []
        for symbol in selected_symbols:
            row_open = trade_map.get(symbol)
            row_next_open = next_trade_map.get(symbol)
            if not row_open or not row_next_open:
                continue
            open_price = float(row_open.get("open", 0.0))
            next_open_price = float(row_next_open.get("open", 0.0))
            if open_price <= 0.0 or next_open_price <= 0.0:
                continue
            tradable_symbols.append(symbol)

        if not tradable_symbols:
            continue

        target_weight = 1.0 / len(tradable_symbols)
        target_weights = {symbol: target_weight for symbol in tradable_symbols}

        period_return = 0.0
        for symbol in tradable_symbols:
            open_price = float(trade_map[symbol]["open"])
            next_open_price = float(next_trade_map[symbol]["open"])
            stock_ret = next_open_price / open_price - 1.0
            period_return += target_weight * stock_ret

        symbols_all = set(prev_weights) | set(target_weights)
        turnover = 0.0
        buy_turnover = 0.0
        sell_turnover = 0.0
        for symbol in symbols_all:
            old_w = prev_weights.get(symbol, 0.0)
            new_w = target_weights.get(symbol, 0.0)
            delta = new_w - old_w
            abs_delta = abs(delta)
            turnover += abs_delta
            if delta > 0:
                buy_turnover += delta
            elif delta < 0:
                sell_turnover += -delta

        trading_cost = buy_turnover * buy_cost + sell_turnover * sell_cost
        net_return = period_return - trading_cost
        nav *= 1.0 + net_return

        hold_days = float(day_to_pos[next_trade_date] - day_to_pos[trade_date])
        records.append(
            {
                "signal_date": signal_date.isoformat(),
                "trade_date": trade_date.isoformat(),
                "next_trade_date": next_trade_date.isoformat(),
                "hold_days": hold_days,
                "gross_return": period_return,
                "turnover": turnover,
                "buy_turnover": buy_turnover,
                "sell_turnover": sell_turnover,
                "trading_cost": trading_cost,
                "net_return": net_return,
                "nav": nav,
            }
        )
        prev_weights = target_weights

    if not records:
        raise ValueError("Backtest result is empty. Please check data and parameters.")

    records.sort(key=lambda item: item["trade_date"])

    running_max = 0.0
    for row in records:
        nav_value = float(row["nav"])
        if nav_value > running_max:
            running_max = nav_value
        drawdown = nav_value / running_max - 1.0 if running_max > 0 else 0.0
        row["cummax_nav"] = running_max
        row["drawdown"] = drawdown

    periods = len(records)
    total_hold_days = sum(float(item["hold_days"]) for item in records)
    total_return = float(records[-1]["nav"] / initial_nav - 1.0)
    annualized = (1.0 + total_return) ** (trading_days_per_year / max(total_hold_days, 1.0)) - 1.0

    avg_hold_days = total_hold_days / periods if periods > 0 else 1.0
    periods_per_year = trading_days_per_year / max(avg_hold_days, 1.0)
    net_returns = [float(item["net_return"]) for item in records]
    vol = _std(net_returns) * _sqrt(periods_per_year)
    sharpe = annualized / vol if vol > 1e-12 else 0.0

    latest_drawdown = float(records[-1]["drawdown"])
    max_drawdown = min(float(item["drawdown"]) for item in records)
    avg_turnover = sum(float(item["turnover"]) for item in records) / periods

    total_turnover = sum(float(item["turnover"]) for item in records)
    total_cost = sum(float(item["trading_cost"]) for item in records)
    cost_ratio = total_cost / total_turnover if total_turnover > 0 else 0.0

    metrics = [
        {"metric": "total_return", "value": total_return},
        {"metric": "annualized_return", "value": annualized},
        {"metric": "annualized_volatility", "value": vol},
        {"metric": "sharpe", "value": sharpe},
        {"metric": "drawdown", "value": latest_drawdown},
        {"metric": "max_drawdown", "value": max_drawdown},
        {"metric": "average_turnover", "value": avg_turnover},
        {"metric": "cost_ratio", "value": cost_ratio},
        {"metric": "total_periods", "value": float(periods)},
    ]

    return records, metrics


def main() -> None:
    cfg = load_config("config.yaml")
    signal_dir = Path(cfg["data"]["prepared_dir"]) / "signals"
    result_dir = ensure_dir(cfg["backtest"]["result_dir"])

    signal_rows = load_signal_rows(signal_dir)
    nav_rows, metrics_rows = run_backtest(cfg, signal_rows)

    nav_path = result_dir / "nav.parquet"
    metrics_path = result_dir / "metrics.parquet"
    write_table(nav_path, nav_rows)
    write_table(metrics_path, metrics_rows)

    print(f"[backtest] records={len(nav_rows)}")
    print(f"[backtest] nav={nav_path}")
    print(f"[backtest] metrics={metrics_path}")
    print("[backtest] done")


if __name__ == "__main__":
    main()

