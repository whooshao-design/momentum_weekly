from __future__ import annotations

from pathlib import Path

from src.momentum_weekly.config_utils import ensure_dir, load_config
from src.momentum_weekly.io_utils import read_table, write_table


def _to_float(value: object) -> float:
    return float(value)


def _pct_change(closes: list[float], idx: int, window: int) -> float:
    if idx < window:
        return 0.0
    base = closes[idx - window]
    if base == 0:
        return 0.0
    return closes[idx] / base - 1.0


def compute_scores(rows: list[dict], mom_windows: list[int], weights: list[float]) -> list[dict]:
    rows.sort(key=lambda item: (item["symbol"], item["date"]))
    result: list[dict] = []

    current_symbol = None
    symbol_rows: list[dict] = []

    def flush_symbol(data_rows: list[dict]) -> None:
        if not data_rows:
            return
        closes = [_to_float(item["close"]) for item in data_rows]
        for idx, item in enumerate(data_rows):
            enriched = dict(item)
            score = 0.0
            for window, weight in zip(mom_windows, weights):
                mom = _pct_change(closes, idx, window)
                enriched[f"mom{window}"] = mom
                score += weight * mom
            enriched["score"] = score
            result.append(enriched)

    for row in rows:
        symbol = row["symbol"]
        if current_symbol is None:
            current_symbol = symbol
        if symbol != current_symbol:
            flush_symbol(symbol_rows)
            symbol_rows = []
            current_symbol = symbol
        symbol_rows.append(row)

    flush_symbol(symbol_rows)
    result.sort(key=lambda item: (item["date"], item["symbol"]))
    return result


def main() -> None:
    cfg = load_config("config.yaml")
    prepared_dir = Path(cfg["data"]["prepared_dir"])
    signal_dir = ensure_dir(prepared_dir / "signals")

    mom_windows = [int(x) for x in cfg["strategy"]["mom_windows"]]
    weights = [float(x) for x in cfg["strategy"]["weights"]]
    if len(mom_windows) != len(weights):
        raise ValueError("mom_windows and weights length mismatch")

    prepared_files = sorted(prepared_dir.glob("prepared_chunk_*.parquet"))
    if not prepared_files:
        raise FileNotFoundError(
            "No prepared chunks found. Please run prepare_data.py first."
        )

    print(
        "[signals] windows=%s weights=%s chunks=%d"
        % (mom_windows, weights, len(prepared_files))
    )
    generated = 0
    for prepared_file in prepared_files:
        rows = read_table(prepared_file)
        out_rows = compute_scores(rows, mom_windows=mom_windows, weights=weights)

        out_file = signal_dir / prepared_file.name.replace("prepared_chunk", "signals_chunk")
        write_table(out_file, out_rows)
        generated += 1
        print(f"[signals] {prepared_file.name} rows={len(out_rows)} -> {out_file.name}")

    print(f"[signals] done. generated_chunks={generated}")


if __name__ == "__main__":
    main()

